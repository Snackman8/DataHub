def text_string(text):
    """
Send back a text string with mime type of /text/plain

TEXT Output:
Example Query: &text=asdf
    """
    return {'body': text, 'content-type': 'text/plain'}
