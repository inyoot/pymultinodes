import pkg_resources
from mako.template import Template
import json

def generate_html(pagename):
    template = Template(filename = pkg_resources.resource_filename('pymultinode', 'html/' + pagename))
    return template.render()

class WebRequestHandler(object):
    def __init__(self, data_source):
        self._data_source = data_source

    def handle_web_request(self, env, start_response):
        if env['PATH_INFO'] == '/':
            start_response('200 OK',[ ('Content-Type', 'text/html') ])
            return [generate_html('index.html')]
        elif env['PATH_INFO'] == '/main.js':
            start_response('200 OK',[ ('Content-Type', 'text/html') ])
            return [pkg_resources.resource_string('pymultinode', 'html/main.js')]
        elif env['PATH_INFO'] == '/jquery.flot.min.js':
            start_response('200 OK',[ ('Content-Type', 'text/html') ])
            return [pkg_resources.resource_string('pymultinode', 'html/jquery.flot.min.js')]

        elif env['PATH_INFO'] == '/data':
            start_response('200 OK',[ ('Content-Type', 'text/plain') ])
            return [json.dumps(self._data_source.data())]
        else:
            start_response('404 NOT FOUND', [])
            return []
