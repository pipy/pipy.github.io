"""生成ページの内部リンク、フラグメント、構造化データ、サイトマップを確認。"""
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, unquote
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = 'https://pipy.github.io'


class Document(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids, self.refs, self.canonical = set(), [], []
        self.h1 = 0
        self.description = False
        self.feed(text)
        self.schema = [json.loads(value) for value in re.findall(r'<script type="application/ld\+json">(.*?)</script>', text, re.S)]

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            assert attrs['id'] not in self.ids, f'duplicate ID: {attrs["id"]}'
            self.ids.add(attrs['id'])
        self.h1 += tag == 'h1'
        if tag == 'meta' and attrs.get('name') == 'description':
            self.description = bool(attrs.get('content'))
        if tag == 'link' and attrs.get('rel') == 'canonical':
            self.canonical.append(attrs['href'])
        for key in ('href', 'src'):
            if key in attrs:
                self.refs.append(attrs[key])


def main():
    paths = list(ROOT.glob('*.html')) + list((ROOT / 'guides').glob('*.html')) + list((ROOT / 'tools').glob('*.html'))
    docs = {path.resolve(): Document(path.read_text()) for path in paths if not path.name.startswith('google')}
    for path, doc in docs.items():
        assert doc.h1 == 1 and doc.description and len(doc.canonical) == 1 and doc.schema, path
        expected = ORIGIN + '/' + ('' if path == ROOT / 'index.html' else str(path.relative_to(ROOT)))
        assert doc.canonical == [expected], (path, doc.canonical)
        for ref in doc.refs:
            parsed = urlsplit(ref)
            if parsed.scheme and not ref.startswith(ORIGIN + '/'):
                continue
            if parsed.netloc:
                target = ROOT / (unquote(parsed.path).lstrip('/') or 'index.html')
            else:
                target = (path.parent / unquote(parsed.path)) if parsed.path else path
            if target.is_dir():
                target = target / 'index.html'
            target = target.resolve()
            assert target.is_file(), f'{path.relative_to(ROOT)}: missing {ref}'
            if parsed.fragment and target in docs:
                assert parsed.fragment in docs[target].ids, f'{path}: missing fragment {ref}'
    sitemap = ElementTree.parse(ROOT / 'sitemap.xml')
    urls = [n.text for n in sitemap.findall('.//{*}loc')]
    assert len(urls) == len(set(urls))
    expected = {d.canonical[0] for p, d in docs.items() if p.name != '404.html'}
    assert set(urls) == expected
    assert f'Sitemap: {ORIGIN}/sitemap.xml' in (ROOT / 'robots.txt').read_text()
    for path in (ROOT / 'tools').glob('*.html'):
        assert 'adsbygoogle' not in path.read_text(), 'Calculators must not load ads'
    assert 'pub-7894074562070938' in (ROOT / 'ads.txt').read_text()
    print(f'PASS: {len(docs)} pages, local links/fragments, metadata, structured data, {len(urls)} sitemap URLs, ad exclusions')


if __name__ == '__main__':
    main()
