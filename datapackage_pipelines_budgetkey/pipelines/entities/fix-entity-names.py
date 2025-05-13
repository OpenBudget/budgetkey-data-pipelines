import dataflows as DF
import re

backslash = re.compile(r'\s*\\\s*')

def fix(name):
    name = backslash.sub(' - ', name)
    return name.strip()

def flow(*_):
    return DF.Flow(
        DF.set_type('name', transform=fix)
    )