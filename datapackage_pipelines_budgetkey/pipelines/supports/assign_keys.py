import re
import dataflows as DF

class Clusterer():

    HEB = re.compile(r'[\u0590-\u05FF]+')

    clusters = dict()
    by_code = dict()
    by_title = dict()
    dedup = dict()
    top_year = 0

    @staticmethod
    def just_hebrew(s):
        """Return only the Hebrew characters in a string."""
        if not s:
            return ''
        return ''.join(c for c in s if Clusterer.HEB.match(c)) or None


    def extract_from_row(self, row):
        budget_code = row.get('budget_code')
        year = row.get('year_requested')
        support_title = row.get('support_title')
        support_title_ = self.just_hebrew(support_title)[:15]
        return budget_code, year, support_title, support_title_

    def process_row(self, row):
        """Generate a key for the row based on budget code and support title."""
        budget_code, year, support_title, support_title_ = self.extract_from_row(row)
        obj = dict(
            code=budget_code,
            title=support_title,
            title_short=support_title_,
            year=year,
        )
        if year > self.top_year:
            self.top_year = year
            print(f'YEAR: {self.top_year}')
        dedup_key = (budget_code, support_title_, year)
        if dedup_key in self.dedup:
            return
    
        # print('SSS', dedup_key)
        top_score = 0
        top_key = None

        candidate_keys = self.by_code.get(budget_code, set()).union(
            self.by_title.get(support_title_, set())
        )

        for k in candidate_keys:
            v = self.clusters.get(k, [])
            for o in v:
                if o['year'] == year:
                    continue
                score = 0
                if o['code'] == budget_code:
                    score += 1
                if o['title_short'] == support_title_:
                    score += 1
                if score > top_score:
                    top_score = score
                    top_key = k
        if top_key is None:
            top_key = f'{budget_code}_{support_title_}'
            assert top_key not in self.clusters, f'Key {top_key} already exists in clusters, {dedup_key} -> {self.dedup}'
            self.clusters[top_key] = list()
            if len(self.clusters) % 100 == 0:
                print(f'Processed {len(self.clusters)} clusters')
        self.dedup[dedup_key] = top_key
        self.by_code.setdefault(budget_code, set()).add(top_key)
        self.by_title.setdefault(support_title_, set()).add(top_key)
        self.clusters[top_key].append(obj)

    def key(self, row):
        """Generate a key for the row and process it."""
        budget_code, year, _, support_title_ = self.extract_from_row(row)
        dedup_key = (budget_code, support_title_, year)
        return self.dedup.get(dedup_key)

    def sorted_keys(self):
        """Return sorted keys of clusters."""
        return sorted(self.clusters.keys(), key=lambda k: self.clusters[k][0]['title_short'])

    def titles_for_key(self, key):
        """Return all titles associated with a given key."""
        return sorted(set(o['title'] for o in self.clusters[key]))

def add_keys(local=False, clusterer=None):
    SOURCE = '/var/datapackages/supports/all' if local else 'https://next.obudget.org/datapackages/supports/all'
    SOURCE += '/datapackage.json'
    clusterer = clusterer or Clusterer()
    DF.Flow(
        DF.load(SOURCE),
        DF.sort_rows('{year_requested}{budget_code}'),
        DF.filter_rows(lambda row: row.get('amount_approved') > 0),
        lambda row: clusterer.process_row(row),
    ).process()
    return DF.Flow(
        DF.load(SOURCE),
        DF.sort_rows('{year_requested}{budget_code}'),
        DF.add_field('program_key', 'string', default=lambda row: clusterer.key(row)),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

def flow(*_):
    return add_keys(local=True)

if __name__ == '__main__':
    clusterer = Clusterer()
    DF.Flow(
        add_keys(local=False, clusterer=clusterer),
        DF.printer(),
        DF.dump_to_path('tmp_add_keys'),
    ).process()

    # for k in clusterer.sorted_keys():
    #     print('\n'.join(clusterer.titles_for_key(k)))
    #     print('-' * 20)
