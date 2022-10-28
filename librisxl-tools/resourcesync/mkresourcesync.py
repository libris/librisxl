from functools import partial
from itertools import islice
from pathlib import Path
from urllib.parse import urljoin
from typing import NamedTuple, Iterable, Optional
from xml.etree import ElementTree as ET
import gzip
import sys
import mimetypes


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
    firstmod: str
    lastmod: str
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
            sync_dir: str,
            outdir: Path,
            max_items=0,
            compress=True,
            representation_templates: list[str]=[],
        ):
        self.outdir = outdir
        self.max_items = max_items or Indexer.DEFAULT_MAX_ITEMS
        self.compress = compress
        self.caplistfilename = f'capabilitylist.xml'
        self.changelistfilename = f'changelist.xml'
        self.descfilename = f'description.xml'

        sync_dir = sync_dir.rstrip('/') + '/' if sync_dir else ''
        sync_base_url = urljoin(base_url, sync_dir)
        self.base_url = base_url
        self.changelist_url = urljoin(sync_base_url, self.changelistfilename)
        self.caplist_url = urljoin(sync_base_url, self.caplistfilename)
        self.desc_url = urljoin(sync_base_url, self.descfilename)

        self.write_sitemap = ItemsetWriter(
                base_url,
                self.changelist_url,
                self.caplist_url,
                representation_templates,
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
        sorteditemsets = sorted(itemsets, key=lambda iset: iset.lastmod)

        if not sorteditemsets:
            return

        firstmod = sorteditemsets[0].firstmod
        lastmod = sorteditemsets[-1].lastmod

        sitemapindex, smsub = elem('sitemapindex')
        smsub('md', ns=RS, capability='changelist',
                 **{'from': firstmod, 'until': lastmod})
        smsub('ln', ns=RS, rel='self', href=self.changelist_url)
        smsub('ln', ns=RS, rel='up', href=self.caplist_url)

        for itemset in sorteditemsets:
            _, urlsetsub = smsub('sitemap')
            urlsetsub('loc', itemset.url)
            urlsetsub('md', ns=RS,
                    **{'from': itemset.firstmod, 'until': itemset.lastmod})
            #urlsetsub('lastmod', itemset.modified) # optional

        outfile = self.outdir / self.changelistfilename

        write_xml(sitemapindex, outfile, self.compress, len(sorteditemsets))

        self.write_parents()

    def write_parents(self):
        self.write_caplist()
        self.write_desc()

    def write_caplist(self):
        root, sub = elem('urlset')
        sub('ln', ns=RS, rel='up', href=self.desc_url)
        sub('ln', ns=RS, rel='self', href=self.caplist_url)
        sub('md', ns=RS, capability='capabilitylist')

        _, urlsub = sub('url')
        urlsub('loc', self.changelist_url)
        urlsub('md', ns=RS, capability='changelist')

        write_xml(root, self.outdir / self.caplistfilename, self.compress)

    def write_desc(self):
        root, sub = elem('urlset')
        sub('ln', ns=RS, rel='self', href=self.desc_url)
        # TOOD: ds_url
        #sub('ln', ns=RS, rel='describedby', href=self.base_url)
        sub('md', ns=RS, capability='description')

        _, urlsub = sub('url')
        urlsub('loc', self.caplist_url)
        urlsub('md', ns=RS, capability='capabilitylist')

        write_xml(root, self.outdir / self.descfilename, self.compress)

        # TODO: Use to enable cheap incremental updates?
        #mapcreatedfile = outfile.parent / 'sitemap-created.tsv'
        #print(f'Writing: {mapcreatedfile}', file=sys.stderr)
        #with open(mapcreatedfile, 'w') as f:
        #    for itemset in sorteditemsets:
        #        print(itemset.file, itemset.created, sep='\t', file=f)


class ItemsetWriter(NamedTuple):

    base_url: str
    changelist_url: str
    caplist_url: str
    representation_templates: list[str]
    compress: bool
    outdir: Path

    def __call__(self, lines: list[str], seqnum: Optional[int] = None) -> ItemSet:
        items = [Item.parse(l) for l in lines]
        firstid = items[0].slug
        firstcreated = items[0].created

        items.sort(key=lambda item: item.modified)

        firstmod = items[0].modified
        lastmod = items[-1].modified

        seqslug = (str(seqnum) if seqnum is not None else
                f"{firstcreated.replace(':', '_').replace('.', '-')}-{firstid}")

        changelist_nosuffix = self.changelist_url.rsplit('.', 1)[0]

        itemlisturl = f'{changelist_nosuffix}-{seqslug}.xml'
        filename = itemlisturl.rsplit('/', 1)[-1]

        sitemap, smsub = elem('urlset')
        smsub('md', ns=RS, capability='changelist',
              **{'from': firstmod, 'until': lastmod})
        smsub('ln', ns=RS, rel='self', href=itemlisturl)
        smsub('ln', ns=RS, rel='index', href=self.changelist_url)
        smsub('ln', ns=RS, rel='up', href=self.caplist_url)

        for slug, created, modified, deleted in items:
            uri = urljoin(self.base_url, slug)
            _, urlsub = smsub('url')
            urlsub('loc', uri)
            if deleted:
                urlsub('md', ns=RS, change='deleted', datetime=modified)
            else:
                #urlsub('lastmod', modified) # optional
                #urlsub('md', ns=RS, at=modified) # in plain resourcelist
                change = 'created' if created == modified else 'updated'
                urlsub('md', ns=RS, change=change, datetime=modified)

                self.add_representations(urlsub, uri)

        write_xml(sitemap, self.outdir / filename, self.compress, len(items))

        return ItemSet(itemlisturl, firstmod, lastmod, firstcreated, filename)

    def add_representations(self, urlsub, uri):
        for repr_url_tplt in self.representation_templates:
            repr_url = repr_url_tplt.format(uri)
            mtype, enc = mimetypes.guess_type(repr_url)

            urlsub('ln', ns=RS, rel='alternate', href=repr_url, type=mtype)


def main():
    """
    This tool generates a set of ResourceSync changeset files from an input
    stream of tab-separated values of the form:

        <slug>\t<created>\t<modified>\t<deleted>

    Where:

    | Column      | Definition |
    | ----------- | ---------- |
    | slug        | Slug ID resolved against BASE_URL
    | created     | SQL or W3C datetime value
    | modified    | SQL or W3C datetime value
    | deleted     | Boolean hint ('t' is true)

    The result is:

    - one Source Description,
    - one Capability List,
    - one Change List index,
    - a set of Change Lists.

    All entries in a Change List are provided in forward chronological order:
    the least recently changed resource at the beginning of the Change List and
    the most recently changed resource must be listed at the end.
    """
    import argparse
    from textwrap import dedent

    argp = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=dedent(main.__doc__),
        )
    argp.add_argument('-b', '--base-url', default='')
    argp.add_argument('-s', '--sync-dir', default='resourcesync/')
    argp.add_argument('-m', '--max-items', type=int, default=0)
    argp.add_argument('-C', '--no-compress', action='store_true')
    argp.add_argument('-M', '--no-multiproc', action='store_true')
    argp.add_argument('-r', '--representation-templates', nargs='*')
    argp.add_argument('outdir', metavar='OUTDIR')
    args = argp.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    indexer = Indexer(args.base_url,
                    args.sync_dir,
                    outdir,
                    args.max_items,
                    compress=not args.no_compress,
                    representation_templates=args.representation_templates)

    if args.no_multiproc:
        indexer.index(sys.stdin)
    else:
        from multiprocessing import Pool
        indexer.index_multiproc(sys.stdin, Pool())


if __name__ == '__main__':
    main()
