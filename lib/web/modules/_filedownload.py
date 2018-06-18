class FileDownloadMixin(object):
    """
    Mixin for providing file downloads.
    """

    def export_content(self, content, filename, content_type = 'text/plain'):
        self.content_type = content_type

        self.additional_headers = [
            ('Content-Disposition', 'attachment; filename="%s"' % filename),
            ('Content-Length', str(len(content))),
            ('Connection', 'close')
        ]

        return content
