from scrapy.dupefilters import RFPDupeFilter
from scrapy import Request


class SessionFilter(RFPDupeFilter):
    def __init__(self, path=None, debug=False):
        super(SessionFilter, self).__init__(path=path, debug=debug)
        self.session_filter = RFPDupeFilter(path=None, debug=debug)

    def request_seen(self, request: Request):
        if request.meta.get('filter_mode', 'undefined') == 'session':
            return self.session_filter.request_seen(request)
        else:
            return super(SessionFilter, self).request_seen(request)
        
    def close(self, reason):
        self.session_filter.close(reason)
        super(SessionFilter, self).close(reason)
