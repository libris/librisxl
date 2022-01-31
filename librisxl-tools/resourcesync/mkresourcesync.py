from functools import partial
from itertools import islice
from pathlib import Path
from typing import NamedTuple, Iterable, Optional
from xml.etree import ElementTree as ET
import gzip
import sys


DEFAULT = 'http://www.sitemaps.org/schemas/sitemap/0.9'
RS = 'http://www.openarchives.org/rs/terms/'

ET.register_namespace('', DEFAULT)
ET.register_namespace('rs', RS)


def qname(ns, lname):
    return str(ET.QName(ns, lname))


def elem(lname, txt=None, ns=DEFAULT, **kws):
    el = ET.Element(qname(ns, lname), **kws)
    if txt is not None:
        el.text = txt
    return el, partial(subelem, el, ns=ns)


def subelem(parent, lname, txt=None, ns=DEFAULT, **kws):
    el = ET.SubElement(parent, qname(ns, lname), **kws)
    if txt is not None:
        el.text = txt
    return el, partial(subelem, el, ns=ns)


def write_xml(root: ET.Element, outfile: Path, compress=False, size=0):
    tree = ET.ElementTree(root)

    if compress:
        sfx = outfile.suffix
        outfile = outfile.with_suffix(f'{sfx}.gz')

    note =  f' ({size:,} items)' if size else ''
    print(f'Writing: {outfile}{note}', file=sys.stderr)

    f = gzip.GzipFile(outfile, 'wb') if compress else outfile.open('wb')
    with f as fp: # type: ignore
        tree.write(fp, encoding='utf-8', xml_declaration=True)


def chunk_by(seq, n):
    it = iter(seq)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            return
        yield chunk



def normalize_timestamp(s : str) -> str:
    s = s.replace('Z', '+00')

    dtime, offsign, tz = s.rpartition('+' if '+' in s else '-')
    dtime = dtime.replace(' ', 'T')

    if '.' in dtime:
        whole, pastdot = dtime.split('.', 1)
        fraction = pastdot
        fraction += '0' * (3 - len(fraction))
        dtime = f"{whole}.{fraction}"
    else:
        dtime += '.0'

    tz = 'Z' if tz == '00' else f"{offsign}{tz}"

    return f'{dtime}{tz}'


class ItemSet(NamedTuple):
    url: str
    modified: str
    created: str
    file: str


class Item(NamedTuple):
    slug: str
    created: str
    modified: str
    deleted: bool

    @classmethod
    def parse(cls, l: str) -> 'Item':
        slug, created, modified, deleted = l.rstrip().split('\t')
        return cls(slug,
                   normalize_timestamp(created),
                   normalize_timestamp(modified),
                   deleted == 't')


class Indexer:

    DEFAULT_MAX_ITEMS = 50_000

    outdir: Path

    def __init__(self,
            base_url: str,
            sync_base: str,
            outdir: Path,
            max_items=0,
            compress=True,
        ):
        self.outdir = outdir
        self.max_items = max_items or Indexer.DEFAULT_MAX_ITEMS
        self.compress = compress
        self.indexfilename = f'sitemap.xml'

        sync_base_url = f'{base_url}/{sync_base}' if sync_base else base_url
        self.index_url = f'{sync_base_url}/{self.indexfilename}'

        self.write_sitemap = ItemsetWriter(base_url,
                sync_base_url,
                self.index_url,
                compress,
                self.outdir)

    def index(self, iterable: Iterable[str]):
        self.dump_sets(self.write_sitemap(lines, i + 1)
                for i, lines in enumerate(
                    chunk_by(iterable, self.max_items)))

    def index_multiproc(self, iterable: Iterable[str], pool):
        self.dump_sets(
                pool.imap_unordered(self.write_sitemap,
                    chunk_by(iterable, self.max_items)))

    def dump_sets(self, itemsets: Iterable[ItemSet]) -> None:
        sorteditemsets = sorted(itemsets, key=lambda iset: iset.modified)

        if not sorteditemsets:
            return

        lastmod = sorteditemsets[-1].modified

        sitemapindex, indexsub = elem('sitemapindex')
        indexsub('md', ns=RS, capability='resourcelist', at=lastmod)
        indexsub('ln', ns=RS, rel='self', href=self.index_url)
        #smsub('ln', ns=RS, rel='up', href=self.capabilitylist)

        for itemset in sorteditemsets:
            _, urlsetsub = indexsub('sitemap')
            urlsetsub('loc', itemset.url)
            urlsetsub('lastmod', itemset.modified)

        outfile = self.outdir / self.indexfilename

        write_xml(sitemapindex, outfile, self.compress, len(sorteditemsets))


class ItemsetWriter(NamedTuple):

    base_url: str
    sync_base_url: str
    index_url: str
    compress: bool
    outdir: Path

    def __call__(self, lines: list[str], seqnum: Optional[int] = None) -> ItemSet:
        items = [Item.parse(l) for l in lines]
        firstid = items[0].slug
        firstcreated = items[0].created

        items.sort(key=lambda item: item.modified)
        lastmod = items[-1].modified

        seqslug = (str(seqnum) if seqnum is not None else
                f"{firstcreated.replace(':', '_').replace('.', '-')}-{firstid}")

        filename = f'sitemap-{seqslug}.xml'

        sitemapurl = f'{self.sync_base_url}/{filename}'

        sitemap, smsub = elem('urlset')
        smsub('md', ns=RS, capability='resourcelist', at=lastmod)
        smsub('ln', ns=RS, rel='self', href=sitemapurl)
        smsub('ln', ns=RS, rel='index', href=self.index_url)
        #smsub('ln', ns=RS, rel='up', href=self.capabilitylist)

        for slug, created, modified, deleted in items:
            modrepr = str(modified)
            uri = f'{self.base_url}/{slug}'
            _, urlsub = smsub('url')
            urlsub('loc', uri)
            if deleted: # TODO: only in a changelist
                # (but we still want to count them to keep the chunks intact?)
                #print('Deleted', uri, 'at', modrepr, file=sys.stderr)
                urlsub('md', ns=RS, change='deleted', datetime=modrepr)
            else:
                urlsub('lastmod', modrepr)
                urlsub('md', ns=RS, at=modrepr)
                urlsub('ln', ns=RS, rel='alternate',
                        href=f'{uri}/data-record.jsonld',
                        type='application/ld+json')

        write_xml(sitemap, self.outdir / filename, self.compress, len(items))

        return ItemSet(sitemapurl, lastmod, firstcreated, filename)


def main():
    """
    This tool generates a set of ResourceSync files from an input stream of
    tab-separated values of the form:

        <slug>\t<created>\t<modified>\t<deleted>

    Where:

    | Column      | Definition |
    | ----------- | ---------- |
    | slug        | Slug ID resolved against BASE_URL
    | created     | SQL or W3C datetime value
    | modified    | SQL or W3C datetime value
    | deleted     | Boolean hint ('t' is true)
    """
    import argparse
    from textwrap import dedent

    argp = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=dedent(main.__doc__),
        )
    argp.add_argument('-b', '--base-url', default='https://libris.kb.se')
    argp.add_argument('-s', '--sync-base', default='sync')
    argp.add_argument('-m', '--max-items', type=int, default=0)
    argp.add_argument('-C', '--no-compress', action='store_true')
    argp.add_argument('-M', '--no-multiproc', action='store_true')
    argp.add_argument('outdir', metavar='OUTDIR')
    args = argp.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    indexer = Indexer(args.base_url,
                    args.sync_base,
                    outdir,
                    args.max_items,
                    compress=not args.no_compress)

    if args.no_multiproc:
        indexer.index(sys.stdin)
    else:
        from multiprocessing import Pool
        indexer.index_multiproc(sys.stdin, Pool())


if __name__ == '__main__':
    main()
