"""build_bodies.merge_known: collected names and codes extend reviewed rows instead of becoming drafts."""
from collections import Counter

from datapackage_pipelines_budgetkey.pipelines.related_items import build_bodies as B
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T


def body(name, aliases=(), codes=()):
    return dict(name=name, aliases=list(aliases), comptroller_codes=list(codes), status='ok')


def found(*entries):
    out = {}
    for name, codes in entries:
        out[T.normalize_name(name)] = dict(names=Counter({name: 1}), codes=set(codes))
    return out


def test_merge_known():
    eapc = body('חברת קו צינור אירופה אסיה בע"מ', codes=['1913'])
    old = body('חברת קו צינור אילת אשקלון בע"מ', aliases=['קו צינור הנפט אילת אשקלון'])
    aviation = body('רשות התעופה האזרחית', codes=['1853'])
    labor = body('משרד העבודה')
    welfare = body('משרד הרווחה', aliases=['משרד העבודה והרווחה'])
    twice_a = body('רשות א', aliases=['הרשות'])
    twice_b = body('רשות ב', aliases=['הרשות'])
    bodies = [eapc, old, aviation, labor, welfare, twice_a, twice_b]
    merged, conflicts = B.merge_known(bodies, found(
        ('קצא"א', ['1913']),                          # a new name under a known code: an alias
        ('רשות התעופה האזרחית', ['1853', '41.16']),  # a known name with a new code too
        ('משרד העבודה', ['07361']),                   # a name, not its substring in another row's alias
        ('קו צינור הנפט אילת אשקלון', ['1913']),      # named by one row, code of another: for review
        ('הרשות', ['5']),                             # an alias of two rows: for review
        ('עיריית שדרות', ['449']),                    # unknown: drafted
    ))
    assert eapc['aliases'] == ['קצא"א'] and merged[T.normalize_name('קצא"א')][1] == 'alias קצא"א'
    assert aviation['comptroller_codes'] == ['1853', '41.16']
    assert labor['comptroller_codes'] == ['07361'] and welfare['comptroller_codes'] == []
    assert {T.normalize_name(n) for n, _, _ in conflicts} == {
        T.normalize_name('קו צינור הנפט אילת אשקלון'), T.normalize_name('הרשות')}
    assert old['comptroller_codes'] == [] and twice_a['comptroller_codes'] == twice_b['comptroller_codes'] == []
    assert T.normalize_name('עיריית שדרות') not in merged
    # idempotent
    again, _ = B.merge_known(bodies, found(('קצא"א', ['1913']), ('רשות התעופה האזרחית', ['1853', '41.16'])))
    assert {c for _, c in again.values()} == {'known'}
