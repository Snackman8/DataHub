""" DataHub Web Application """
# --------------------------------------------------
#    Imports
# --------------------------------------------------
import argparse
import logging
import sys
import traceback
from urllib.parse import urlparse, parse_qs
import business_logic
from pylinkjs.PyLinkJS import run_pylinkjs_app


# --------------------------------------------------
#    Constants
# --------------------------------------------------
DEFAULT_PORT = 9151


# --------------------------------------------------
#    Handlers
# --------------------------------------------------
def ready(jsc, *args):
    """ called when a webpage creates a new connection the first time on load """
    pass


def handle_404(path, uri, host, extra_settings, *args):
    """ called when a .html file can not be found for the path

        returns data, the content_type for the data, i.e. "text/plain" and an HTTP return code

        Args:
            path - path to load for which no .html file was found
            uri - full uri with parameters, i.e. /a/b?c=d&e=f
            host - host which is handling this request
            extra_settings - extra setings from the application

        Returns:
            result, content_type, return_code
    """
    # retrieve the qid, auithuser, authtoken, and callerid
    parsed_url = urlparse(uri)
    parsed_qs = parse_qs(parsed_url.query)
    qid = parsed_qs.get('qid', '')

    # retrieve security tokens
    authuser = parsed_qs.pop('authuser', [''])[0]
    authtoken = parsed_qs.pop('authtoken', [''])[0]
    callerid = parsed_qs.pop('callerid', [''])[0]

    try:
        # check if the qid parameter was passed in
        if qid == '':
            # try to display the html docs for the module
            html, content_type, return_code = business_logic.build_html_docs(host, path, extra_settings['modulepath'],
                                                                             authuser, authtoken, callerid)
            return (html, content_type, return_code)
        else:
            # execute the query
            parsed_qs = {k:v[0] for k, v in parsed_qs.items()}
            html, content_type, return_code = business_logic.execute_query(path, parsed_qs)
            return (html, content_type, return_code)
    except:
        html = f'<pre>{traceback.format_exc()}</pre>'
        return (html, 'text/html', 500)


# --------------------------------------------------
#    Main
# --------------------------------------------------
def main():
    # parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="port to run on", default=DEFAULT_PORT, required=False)
    parser.add_argument("--modulepath", help="location of additional modules", default='.', required=False)
    args = vars(parser.parse_args())
    print(args)

    # modify the sys path if needed
    if args['modulepath'] != '.':
        sys.path.append(args['modulepath'])

    # run the application
    logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(threadName)s %(message)s')
    run_pylinkjs_app(default_html='this_should_never_exist', on_404=handle_404, port=args['port'],
                     extra_settings={'modulepath': args['modulepath']})


if __name__ == '__main__':
    main()
    