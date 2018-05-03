class HTMLMixin(object):
    header_html = None
    footer_html = None

    def __init__(self, title, body):
        # Page title
        self.title = title
        # List of additional CSS
        self.stylesheets = None
        # CSS text to write directly in the header
        self.header_stylesheet = None
        # List of scripts to be imported with src=
        self.scripts = None
        # Script code to be executed in the header
        self.header_script = None
        # Additional components in the title block
        self.titleblock = None
        # Body HTML
        with open(body) as source:
            self.body_html = source.read()

    def form_html(self, repl):
        """
        Combine header, body, footer and perform string replacements.
        """

        repl['_TITLE_'] = self.title

        if self.stylesheets is not None:
            text = '\n'
            for path in self.stylesheets:
                text += '    <link href="' + path + '" rel="stylesheet">\n'
            repl['_STYLESHEETS_'] = text
        else:
            repl['_STYLESHEETS_'] = ''

        if self.header_stylesheet is not None:
            repl['_HEADER_STYLESHEET_'] = '\n    <style>\n' + self.header_stylesheet + '\n</style>'
        else:
            repl['_HEADER_STYLESHEET_'] = ''

        if self.scripts is not None:
            text = '\n'
            for path in self.scripts:
                text += '    <script type="text/javascript" src="' + path + '"></script>\n'
            repl['_SCRIPTS_'] = text
        else:
            repl['_SCRIPTS_'] = ''

        if self.header_script is not None:
            repl['_HEADER_SCRIPT_'] = '\n    <script type="text/javascript">\n' + self.header_script + '\n</script>'
        else:
            repl['_HEADER_SCRIPT_'] = ''

        if self.titleblock is not None:
            repl['_TITLEBLOCK_'] = '\n' + self.titleblock

        return (HTMLMixin.header_html + self.body_html + HTMLMixin.footer_html).format(**repl)
