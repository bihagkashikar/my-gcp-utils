import json
from pathlib import Path
#from docopt import docopt
from jsonref import replace_refs

def load_json(file):
    with open(file, "r") as f:
        data = json.load(f)
    return data

def write_to_file(data):
    TARGETFILE = "/home/ubuntu/work/projects/mygithubrepos/my-gcp-utils/bigquery-utils/bq-table-schema-utils/convert_drefs_jsonschema_to_bqschema/sample_dref_removed_schema.json" 
    with open(TARGETFILE, "a",) as dref:
        dref.writelines(data)

if __name__ == "__main__":

    JSONFILE = "/home/ubuntu/work/projects/mygithubrepos/my-gcp-utils/bigquery-utils/bq-table-schema-utils/convert_drefs_jsonschema_to_bqschema/sample_dfref_source_schema.json"

    # replace_refs returns a copy of the document with refs replaced by JsonRef
    # objects.  It will resolve refences to other JSON schema files
    doc = replace_refs(
        load_json(JSONFILE),
        merge_props=True,
        base_uri=Path(JSONFILE).absolute().as_uri(),
    )
    print(json.dumps(doc, indent=2))
    write_to_file(json.dumps(doc, indent=2))