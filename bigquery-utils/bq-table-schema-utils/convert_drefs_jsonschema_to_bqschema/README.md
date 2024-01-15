<h1>Pre-requisites</h1>

<h2>Package installations</h2>

**Install npm**

`sudo dnf module install nodejs:18/common`

**Install npm package**

`sudo npm install @apidevtools/json-schema-ref-parser`

**Install npm package - For reference, follow instructions on this source repo https://github.com/thedumbterminal/jsonschema-bigquery**

`sudo npm install jsonschema-bigquery`

**Install python jsonref package**

`python -m pip install jsonref `

**Ensure to run Google Cloud Auth**

`gcloud auth login`
`gcloud auth application-default login`

<h1>Usage</h1>

1. Ensure the schema to be de-references is copied into a file in the directory

2. Edit the `replace_drefs.py` and make sure the TARGETFILE and JSONFILE path point to relative folder path.

3. Run `python -m replace_drefs.py`

4. Edit the `convert_dref_to_bq_schema.sh` and replace the `source` and `target` file path where source path is the file generated from step above, and target path is path where the `npx jsbq` package will generate the bq compatible schema