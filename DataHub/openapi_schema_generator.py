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
    returns_section = []

    for line in docstring.splitlines():
        stripped = line.strip()

        if not stripped:  # Skip empty lines
            continue

        if stripped.lower().startswith("params:") or stripped.lower().startswith("parameters:"):
            parameters_section = []
            is_description = False
            current_section = "Parameters"
            continue

        if stripped.lower().startswith("returns:"):
            returns_section = []
            is_description = False
            current_section = "Returns"
            continue

        if stripped.lower().startswith("example output:"):
            current_section = "Example Output"
            continue

        # Stop adding to description when we hit any recognized section
        if current_section != 'Returns':
            if ":" in stripped and stripped.endswith(":"):
                current_section = None
                continue

        if is_description:
            # Accumulate as part of the description
            description += f"{stripped} "
        elif current_section == "Parameters":
            # Collect lines in the Params section
            parameters_section.append(stripped)
        elif current_section == "Returns":
            # Collect lines in the Params section
            returns_section.append(stripped)
        elif current_section == "Example Output":
            # Collect lines in the Example Output section
            example_output += stripped + "\n"

    # Parse parameters from the 'Params:' section
    parameters = {}
    if parameters_section:
        for line in parameters_section:
            if ":" in line:
                param_name, param_desc = line.split(":", 1)
                param_name = param_name.strip()
                param_desc = param_desc.strip()
                if '(' in param_name and ')' in param_name:
                    param_type = param_name[param_name.find('('):]  # Extract type including parentheses
                    param_name = param_name[:param_name.find('(')].strip()  # Remove type from param_name
                    param_desc = param_type + ' ' + param_desc  # Prepend type to param_desc
                parameters[param_name] = param_desc
            elif "-" in line:
                param_name, param_desc = line.split("-", 1)
                param_name = param_name.strip()
                param_desc = param_desc.strip()
                parameters[param_name] = param_desc

    returns_schema = {"type": "object", "properties": {}}

    # Extract the overall description from the first line
    if ":" in returns_section[0]:
        returns_schema["description"] = returns_section[0].split(": ", 1)[1]

    for line in returns_section[1:]:  # Process remaining lines
        if line.startswith('- ') and ': ' in line:
            key_type_desc = line.strip("- ").split(": ")
            key_type, desc = key_type_desc[0].strip("'"), key_type_desc[1]
            key, type_name = key_type.split("'")
            type_name = type_name.strip().strip('(').strip(')')

            # Map Python types to OpenAPI types
            type_mapping = {"str": "string", "int": "integer", "bool": "boolean", "float": "number", "list": "array", "dict": "object"}
            returns_schema["properties"][key] = {
                "type": type_mapping.get(type_name, "string"),
                "description": desc
            }

    # Clean and normalize the example output
    if example_output:
        example_output = "\n".join(line.strip() for line in example_output.splitlines())

    return description.strip(), parameters, returns_schema, example_output.strip()


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
                            description, param_docs, returns_schema, example_output = parse_docstring(docstring or "")

                            # Extract parameters
                            post_param_schema = {'type': 'object', 'properties': {}}
#                            parameters = []
                            type_mapping = {"str": "string", "int": "integer", "bool": "boolean", "float": "number", "list": "array", "dict": "object"}
                            for name, param_desc in param_docs.items():
                                param_type = "string"  # Default type
                                if "(" in param_desc and ")" in param_desc:
                                    param_type = param_desc.split("(")[1].split(")")[0]
                                    param_desc = param_desc.split(")")[1].strip()

                                post_param_schema['properties'][name] = {
                                    "type": type_mapping.get(param_type, param_type),
                                    "description": param_desc,
                                    }

                                # parameters.append({
                                #     "name": name,
                                #     "in": "query",
                                #     "required": True,  # Assume required by default
                                #     "schema": {"type": python_to_openapi_types.get(param_type, "string")},
                                #     "description": param_desc,
                                # })

                            # Generate OpenAPI operation ID
                            relative_path = os.path.relpath(root, start_path)
                            if relative_path == '.':
                                relative_path = ''
                            operation_id = f"{relative_path.replace('/', '__')}__{module_name}__{func_name}".strip("_")
                            api_path = "/" + f"/{relative_path}/{module_name}/{func_name}".strip('/')

                            openapi_schema["paths"].setdefault(api_path, {
                                # "get": {
                                #     "summary": f"Handler for {module_name}.{func_name}",
                                #     "description": description or "",
                                #     "operationId": operation_id,
                                #     "parameters": parameters,
                                #     "responses": {
                                #         "200": {
                                #             "description": "Successful Response",
                                #             "content": {
                                #                 "application/json": {
                                #                     "schema": returns_schema,
                                #                 }
                                #             }
                                #         }
                                #     },
                                # }
                                "post": {
#                                    "summary": f"Handler for {module_name}.{func_name}",
                                    "description": description or "",
                                    "operationId": operation_id,
 #                                   "parameters": parameters,
                                    "requestBody": {
                                        "content": {
                                            "application/json": {
                                                "schema": post_param_schema,
                                            }
                                        }
                                    },
                                    "responses": {
                                        "200": {
                                            "description": "Successful Response",
                                            "content": {
                                                "application/json": {
                                                    "schema": returns_schema,
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
