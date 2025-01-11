import os
import importlib
import inspect
import yaml
from fastapi.openapi.utils import get_openapi


def parse_docstring(docstring):
    """
    Parse a docstring to extract:
    - The description (everything before 'Params:').
    - The parameters (from 'Params:' section only).
    - The example output (from 'Example Output:' section).
    """
    description = ""
    parameters_section = None
    example_output = ""
    is_description = True
    current_section = None

    for line in docstring.splitlines():
        stripped = line.strip()

        if not stripped:  # Skip empty lines
            continue

        if stripped.lower().startswith("params:"):
            parameters_section = []
            is_description = False
            current_section = "Params"
            continue

        if stripped.lower().startswith("example output:"):
            current_section = "Example Output"
            continue

        # Stop adding to description when we hit any recognized section
        if ":" in stripped and stripped.endswith(":"):
            current_section = None
            continue

        if is_description:
            # Accumulate as part of the description
            description += f"{stripped} "
        elif current_section == "Params":
            # Collect lines in the Params section
            parameters_section.append(stripped)
        elif current_section == "Example Output":
            # Collect lines in the Example Output section
            example_output += stripped + "\n"

    # Parse parameters from the 'Params:' section
    parameters = {}
    if parameters_section:
        for line in parameters_section:
            if "-" in line:
                param_name, param_desc = line.split("-", 1)
                param_name = param_name.strip()
                param_desc = param_desc.strip()
                parameters[param_name] = param_desc

    # Clean and normalize the example output
    if example_output:
        example_output = "\n".join(line.strip() for line in example_output.splitlines())

    return description.strip(), parameters, example_output.strip()




def generate_openapi_schema(start_path, server_url):
    """Generate OpenAPI schema dynamically based on discovered modules and functions."""
    openapi_schema = get_openapi(
        title="Dynamic API",
        version="1.0.0",
        description="API with dynamic function handlers",
        routes=[],
    )

    openapi_schema["servers"] = [{"url": server_url}]

    python_to_openapi_types = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
    }

    for root, _, files in os.walk(start_path):
        for file in files:
            if file.endswith(".py") and not file.startswith("_"):
                module_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)

                try:
                    spec = importlib.util.spec_from_file_location(module_name, file_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    for func_name, func in inspect.getmembers(module, inspect.isfunction):
                        try:
                            original_func = getattr(func, "__wrapped__", func)
                            if original_func.__module__ != module.__name__:
                                continue

                            if func_name.startswith('_'):
                                continue

                            docstring = inspect.getdoc(original_func)

                            # Parse docstring dynamically
                            description, param_docs, example_output = parse_docstring(docstring or "")

                            # Extract parameters
                            parameters = []
                            for name, param_desc in param_docs.items():
                                param_type = "string"  # Default type
                                if "(" in param_desc and ")" in param_desc:
                                    param_type = param_desc.split("(")[1].split(")")[0]
                                    param_desc = param_desc.split(")")[1].strip()

                                parameters.append({
                                    "name": name,
                                    "in": "query",
                                    "required": True,  # Assume required by default
                                    "schema": {"type": python_to_openapi_types.get(param_type, "string")},
                                    "description": param_desc,
                                })

                            # Add example output to the response schema
                            response_schema = {
                                "type": "string",
                                "format": "binary",
                            }
                            if example_output:
                                response_schema["example"] = example_output

                            # Generate OpenAPI operation ID
                            relative_path = os.path.relpath(root, start_path)
                            if relative_path == '.':
                                relative_path = ''
                            operation_id = f"{relative_path.replace('/', '__')}__{module_name}__{func_name}".strip("_")
                            api_path = f"/{relative_path}/{module_name}/{func_name}".strip('/')

                            openapi_schema["paths"].setdefault(api_path, {
                                "get": {
                                    "summary": f"Handler for {module_name}.{func_name}",
                                    "description": description or "",
                                    "operationId": operation_id,
                                    "parameters": parameters,
                                    "responses": {
                                        "200": {
                                            "description": "Successful Response",
                                            "content": {
                                                "text/csv": {  # Use 'text/csv' content type for CSV output
                                                    "schema": response_schema,
                                                }
                                            }
                                        }
                                    },
                                }
                            })

                        except Exception as e:
                            print(f"Error processing function {func_name} in module {module_name}: {e}")

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

    return yaml.dump(openapi_schema, default_flow_style=False)


