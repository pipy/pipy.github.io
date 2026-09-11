"""標準ライブラリだけで共通レイアウト付きの静的HTMLを生成する。"""
import json
import sys
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from content.pages import PAGES

ORIGIN = 'https://pipy.github.io/'
UPDATED = '2026-09-11'
NAME = '空売りデータノート'
ARTICLES = json.loads((ROOT / 'content/articles.json').read_text())
URLS = []


def page(path, title, description, body, active='', script=None, article=None, ads=False, noindex=False):
    prefix = ORIGIN if path == '404.html' else ('../' if '/' in path else '')
    canonical = ORIGIN + ('' if path == 'index.html' else path)
    nav = [('index.html', '銘柄検索', 'home'), ('guides/index.html', '学ぶ', 'guides'),
           ('tools/index.html', '計算ツール', 'tools'), ('glossary.html', '用語集', 'glossary')]
    links = ''.join(f'<a href="{prefix}{url}"' + (' aria-current="page"' if active == key else '') + f'>{label}</a>' for url, label, key in nav)
    footer = ''.join(f'<a href="{prefix}{url}">{label}</a>' for url, label in [
        ('about.html', '運営者・編集方針'), ('methodology.html', '出典・集計方法'), ('contact.html', 'お問い合わせ'),
        ('privacy.html', 'プライバシーポリシー'), ('disclaimer.html', '免責事項')])
    schema = {'@context': 'https://schema.org', '@type': 'WebSite' if path == 'index.html' else 'WebPage',
              'name': title, 'url': canonical, 'inLanguage': 'ja', 'description': description}
    if article:
        schema.update({'@type': 'Article', 'headline': title, 'dateModified': UPDATED,
                       'author': {'@type': 'Person', 'name': 'pipy', 'url': ORIGIN + 'about.html'},
                       'mainEntityOfPage': canonical})
    adtag = '<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-7894074562070938" crossorigin="anonymous"></script>' if ads else ''
    html = f'''<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} | {NAME}</title><meta name="description" content="{escape(description, quote=True)}">
<meta name="robots" content="{'noindex,follow' if noindex else 'index,follow'}"><link rel="canonical" href="{canonical}">
<meta property="og:type" content="{'article' if article else 'website'}"><meta property="og:locale" content="ja_JP"><meta property="og:site_name" content="{NAME}"><meta property="og:title" content="{escape(title, quote=True)}"><meta property="og:description" content="{escape(description, quote=True)}"><meta property="og:url" content="{canonical}">
<meta name="theme-color" content="#142c41"><link rel="icon" href="{prefix}assets/favicon.svg" type="image/svg+xml"><link rel="stylesheet" href="{prefix}assets/site.css">
<script type="application/ld+json">{json.dumps(schema, ensure_ascii=False).replace('<', chr(92) + 'u003c')}</script>{adtag}
{f'<script defer src="{prefix}assets/{script}"></script>' if script else ''}</head><body>
<a class="skip-link" href="#main">本文へ移動</a><header class="site-header"><div class="wrap header-inner"><a class="brand" href="{prefix}index.html"><span class="brand-mark" aria-hidden="true">空</span>{NAME}</a><nav class="site-nav" aria-label="メインナビゲーション">{links}</nav></div></header>
<main id="main" class="wrap">{body}</main><footer class="site-footer"><div class="wrap"><div class="footer-inner"><div><strong>{NAME}</strong><p>公開データと、数字を読むための知識。</p></div><nav class="footer-links" aria-label="運営情報">{footer}</nav></div><p>© pipy · 情報提供・学習を目的とし、特定銘柄の売買を推奨しません。</p></div></footer></body></html>'''
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(html, encoding='utf-8')
    if not noindex:
        URLS.append(canonical)


def cards(items, prefix='', heading='h3'):
    return '<div class="cards">' + ''.join(f'''<article class="card"><span class="category">{a['category']}</span><{heading}><a href="{prefix}{a['slug']}.html">{a['title']}</a></{heading}><p>{a['description']}</p><span class="meta">解説 · {max(3, len(str(a['sections'])) // 500)}分</span></article>''' for a in items) + '</div>'


def main():
    home = (ROOT / 'templates/home.html').read_text().replace('{{FEATURED}}', cards([ARTICLES[i] for i in [0, 2, 5]], 'guides/'))
    page('index.html', '機関空売り残高を検索・株式データを学ぶ', 'JPXの公表空売り残高を銘柄別に検索。信用取引・出来高・決算の読み方を解説記事と計算ツールで学べます。', home, 'home', 'viewer.js', ads=True)
    categories = list(dict.fromkeys(a['category'] for a in ARTICLES))
    listing = '<div class="page-heading"><p class="eyebrow">LEARNING LIBRARY</p><h1>株式データの読み方を学ぶ</h1><p class="intro">空売り残高から企業の決算まで。数値の意味と、そこから分からないことを整理する解説です。</p></div><nav class="filters" aria-label="記事の分野">' + ''.join(f'<a href="#category-{i}">{c}</a>' for i, c in enumerate(categories)) + '</nav>'
    for i, c in enumerate(categories):
        listing += f'<section id="category-{i}" class="guide-category"><div class="section-title"><h2>{c}</h2></div>' + cards([a for a in ARTICLES if a['category'] == c]) + '</section>'
    page('guides/index.html', '解説記事一覧', '空売り、信用取引、株式の基礎、企業を読むための解説記事を分野別に探せます。', listing, 'guides')
    for i, a in enumerate(ARTICLES):
        toc = '<aside class="toc" aria-label="記事の目次"><strong>この記事の内容</strong><ol>' + ''.join(f'<li><a href="#section-{n}">{s[0]}</a></li>' for n, s in enumerate(a['sections'])) + '</ol><a href="../glossary.html">用語集を開く</a></aside>'
        sections = ''.join(f'<section id="section-{n}"><h2>{s[0]}</h2>{s[1]}</section>' for n, s in enumerate(a['sections']))
        sources = '<section class="sources"><h2>参考にした一次資料</h2><ul>' + ''.join(f'<li><a href="{url}">{name}</a></li>' for name, url in a['sources']) + '</ul><p>数値例と読み解き方は当サイトの説明です。制度・公表方法の詳細はリンク先で確認してください。</p></section>'
        body = f'<nav class="breadcrumb" aria-label="パンくず"><a href="../index.html">ホーム</a> / <a href="index.html">学ぶ</a> / {a["category"]}</nav><div class="article-layout"><article class="article"><span class="eyebrow">{a["category"]}</span><h1>{a["title"]}</h1><p class="meta">執筆・編集：<a href="../about.html">pipy</a> · 更新 <time datetime="{UPDATED}">{UPDATED}</time></p><p class="lead">{a["description"]}</p>{sections}{sources}<p class="reading-note">一般的な情報提供・学習のための解説です。個別の投資助言ではありません。誤りのご連絡は<a href="../contact.html">訂正依頼</a>へ。</p></article>{toc}</div><div class="section-title"><h2>あわせて読む</h2><a href="index.html">記事一覧</a></div>'
        related = [ARTICLES[(i + j) % len(ARTICLES)] for j in (1, 2, 3)]
        page(f'guides/{a["slug"]}.html', a['title'], a['description'], body + cards(related), 'guides', article=a, ads=True)
    for path, (title, desc, content) in PAGES.items():
        body = f'<nav class="breadcrumb" aria-label="パンくず"><a href="index.html">ホーム</a> / {title}</nav><article class="article" style="max-width:850px;margin:auto"><h1>{title}</h1><p class="meta">更新：<time datetime="{UPDATED}">{UPDATED}</time> · 運営：pipy</p>{content}</article>'
        page(path, title, desc, body, 'glossary' if path == 'glossary.html' else '')
    for slug, title, desc in [('short-profit', '空売り損益の概算', '売値・買戻し価格・日数・仮の費用から税引前損益を試算します。'), ('change-rate', '増減率とポイント差の計算', '株数の増減率と、割合のパーセントポイント差を区別して計算します。')]:
        content = (ROOT / f'templates/{slug}.html').read_text()
        body = f'<nav class="breadcrumb" aria-label="パンくず"><a href="../index.html">ホーム</a> / <a href="index.html">計算ツール</a></nav><article class="article" style="max-width:880px;margin:auto"><p class="eyebrow">CALCULATOR</p><h1>{title}</h1><p class="lead">{desc}</p>{content}</article>'
        page(f'tools/{slug}.html', title, desc, body, 'tools', 'calculators.js')
    page('tools/index.html', '学習用の計算ツール', '価格や費用、残高の増減を自分の条件で計算して、数字の意味を確かめます。', '<div class="page-heading"><p class="eyebrow">CALCULATORS</p><h1>数字を手元で確かめる</h1><p class="intro">条件を変えながら、計算結果の違いを確かめます。入力値を送信・保存する処理はありません。</p></div><div class="two-col"><section class="card"><h2><a href="short-profit.html">空売り損益の概算</a></h2><p>価格差に加え、貸株料の単純な概算とその他費用を差し引きます。価格上昇や保有期間の延長も試せます。</p><a href="short-profit.html">損益を計算する</a></section><section class="card"><h2><a href="change-rate.html">増減率とポイント差</a></h2><p>前回と今回の数値から変化を計算します。分母がゼロの場合や、割合どうしの差も確認できます。</p><a href="change-rate.html">増減を計算する</a></section></div><section class="info-note" style="margin-top:28px"><h2>計算の前提を確認する</h2><p>ツールは学習用です。実際の取引費用や精算額を保証しません。各ページに計算式と対象外の条件を記載しています。</p><a href="../guides/short-selling-costs.html">空売りの損益と費用の解説</a></section>', 'tools')
    page('404.html', 'ページが見つかりません', 'ページのURLをご確認ください。', '<article class="article"><p class="eyebrow">404</p><h1>ページが見つかりません</h1><p>URLが変わったか、入力に誤りがある可能性があります。</p><div class="actions"><a class="button" href="https://pipy.github.io/">銘柄検索へ</a><a class="button secondary" href="https://pipy.github.io/guides/index.html">解説記事を読む</a></div></article>', noindex=True)
    (ROOT / 'sitemap.xml').write_text('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + ''.join(f'  <url><loc>{u}</loc></url>\n' for u in URLS) + '</urlset>\n')
    (ROOT / 'robots.txt').write_text(f'User-agent: *\nAllow: /\nSitemap: {ORIGIN}sitemap.xml\n')
    print(f'Generated {len(URLS)} indexable pages, 404.html and sitemap.xml')


if __name__ == '__main__':
    main()
