import re
import pathlib
import itertools
import collections

from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec
from pycldf import Source


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "sails"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(module='StructureDataset', dir=self.cldf_dir)

    def cmd_download(self, args):
        pass

    def read(self, core, extended=False, pkmap=None, key=None):
        if not key:
            key = lambda d: int(d['pk'])
        res = collections.OrderedDict()
        for row in sorted(self.raw_dir.read_csv('{0}.csv'.format(core), dicts=True), key=key):
            res[row['pk']] = row
            if pkmap is not None:
                pkmap[core][row['pk']] = row['id']
        if extended:
            for row in self.raw_dir.read_csv('{0}.csv'.format(extended), dicts=True):
                res[row['pk']].update(row)
        return res

    def itersources(self, pkmap):
        for row in self.raw_dir.read_csv('source.csv', dicts=True):
            del row['jsondata']
            pkmap['source'][row.pop('pk')] = row['id']
            row['title'] = row.pop('description')
            row['key'] = row.pop('name')
            yield Source(row.pop('bibtex_type'), row.pop('id'), **row)

    def cmd_makecldf(self, args):
        self.create_schema(args.writer.cldf)
        pk2id = collections.defaultdict(dict)

        args.writer.cldf.add_sources(*list(self.itersources(pk2id)))
        for row in self.read('designer').values():
            id_ = re.search('\(([A-Z]+)\)', row['domain']).groups()[0]
            args.writer.objects['contributions.csv'].append({
                'ID': id_,
                'Name': row['domain'],
                'Contributor': row['contributor'],
                'Related_Resource_URL': row['pdflink'],
                'Citation': row['citation'],
                'Related_Resource': row['more_information'],
                'Orientation': row['orientation'],
            })
        args.writer.objects['contributions.csv'].sort(key=lambda d: d['ID'])

        d2c = {
            r['name']: r['contribution'] for r in self.etc_dir.read_csv('domains.csv', dicts=True)}
        domains = self.read('featuredomain')

        for row in self.read(
                'parameter', extended='feature', pkmap=pk2id, key=lambda d: d['id']).values():
            args.writer.objects['ParameterTable'].append({
                'ID': row['id'],
                'Name': row['name'],
                'Description': row['description'],
                'Domain': domains[row['featuredomain_pk']]['name'],
                'Contribution_ID': d2c[domains[row['featuredomain_pk']]['name']],
            })

        def code_id(s):
            return s.replace('?', 'NK').replace('N/A', 'NA')

        for row in self.read(
                'domainelement',
                pkmap=pk2id,
                key=lambda d: d['id']).values():
            args.writer.objects['CodeTable'].append({
                'ID': code_id(row['id']),
                'Parameter_ID': pk2id['parameter'][row['parameter_pk']],
                'Name': row['name'],
                'Description': row['description'],
            })

        families = self.read('family')
        glang = {l.hid: (l.id, l.iso) for l in args.glottolog.api.languoids() if l.hid}
        for row in self.read(
                'language', extended='sailslanguage', pkmap=pk2id, key=lambda d: d['id']).values():
            args.writer.objects['LanguageTable'].append({
                'ID': row['id'],
                'Name': row['name'],
                'ISO639P3code': glang[row['id']][1],
                'Glottocode': glang[row['id']][0],
                'Latitude': row['latitude'],
                'Longitude': row['longitude'],
                'Family': families[row['family_pk']]['name'],
            })
        args.writer.objects['LanguageTable'].sort(key=lambda d: d['ID'])

        refs = {
            vspk: sorted(pk2id['source'][r['source_pk']] for r in rows)
            for vspk, rows in itertools.groupby(
                self.read('valuesetreference', key=lambda d: d['valueset_pk']).values(),
                lambda d: d['valueset_pk'],
            )
        }
        vsdict = self.read('valueset', pkmap=pk2id)
        for row in self.read('value', extended='sailsvalue').values():
            vs = vsdict[row['valueset_pk']]
            args.writer.objects['ValueTable'].append({
                'ID': vs['id'],
                'Language_ID': pk2id['language'][vs['language_pk']],
                'Parameter_ID': pk2id['parameter'][vs['parameter_pk']],
                'Value': pk2id['domainelement'][row['domainelement_pk']].split('-')[1],
                'Code_ID': code_id(pk2id['domainelement'][row['domainelement_pk']]),
                'Comment': row['comment'],
                'Source': refs.get(row['valueset_pk'], []),
                'Reference': vs['source'],
                'Example_Reference': row['example'],
                'Contributor': row['contributed_datapoint'],
            })

        args.writer.objects['ValueTable'].sort(
            key=lambda d: (d['Language_ID'], d['Parameter_ID']))


    def create_schema(self, cldf):
        cldf.add_component(
            'ParameterTable',
            'Domain',
            'Contribution_ID',
        )
        cldf.add_component(
            'CodeTable',
        )
        cldf.add_component('LanguageTable')
        cldf.add_table(
            'contributions.csv',
            'ID',
            'Name',
            'Orientation',
            {
                'name': 'Contributor',
                'propertyUrl': 'http://purl.org/dc/terms/creator',
            },
            {
                'name': 'Citation',
                'propertyUrl': 'http://purl.org/dc/terms/bibliographicCitation',
            },
            {
                'name': 'Related_Resource',
                'propertyUrl': 'http://purl.org/dc/elements/1.1/relation',
            },
            {
                'name': 'Related_Resource_URL',
            },
        )
        cldf.add_columns('ValueTable', 'Contributor', 'Reference', 'Example_Reference')
        cldf.add_foreign_key('ParameterTable', 'Contribution_ID', 'contributions.csv', 'ID')
