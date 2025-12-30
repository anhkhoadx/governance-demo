import typer
from rich import print
from rich.panel import Panel

from govdemo.pipelines.init import run_init
from govdemo.pipelines.seed import run_seed
from govdemo.pipelines.ingest import run_ingest
from govdemo.pipelines.clean import run_clean
from govdemo.pipelines.curate import run_curate
from govdemo.pipelines.serve import run_serve
from govdemo.pipelines.identity import run_build_identity
from govdemo.pipelines.export import run_export_audience
from govdemo.pipelines.gdpr import request_delete

app = typer.Typer(add_completion=False)

@app.command("init")
def init_cmd():
    res = run_init()
    print(Panel.fit(f"Initialized lake + audit DB\nlake_root: {res['lake_root']}\nduckdb: {res['duckdb']}\nlineage: {res['lineage']}"))

@app.command("seed")
def seed_cmd():
    res = run_seed()
    print(f"Seeded landing file {res['landing_file']} ({res['rows']} rows)")

@app.command("ingest")
def ingest_cmd(source: str = "app", dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_ingest(source=source, dt=dt)
    print(f"Ingest complete run_id={res['run_id']}")
    print(f"raw: {res['raw_path']} ({res['good']} rows)")
    print(f"quarantine: {res['quarantine_path']} ({res['bad']} rows)")

@app.command("clean")
def clean_cmd(dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_clean(dt=dt)
    print(f"Clean complete run_id={res['run_id']}")
    print(f"clean: {res['clean_path']} ({res['rows']} rows)")

@app.command("curate")
def curate_cmd(dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_curate(dt=dt)
    print(f"Curate complete run_id={res['run_id']}")
    print(f"curated: {res['curated_path']} ({res['rows']} rows)")

@app.command("serve")
def serve_cmd(dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_serve(dt=dt)
    print(f"Serve complete run_id={res['run_id']}")
    print(f"serving: {res['serving_path']} ({res['rows']} rows)")

@app.command("build-identity")
def identity_cmd(dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_build_identity(dt=dt)
    print(f"Identity build complete run_id={res['run_id']}")
    print(f"restricted_pii: {res['identity_path']} ({res['rows']} rows)")

@app.command("export-audience")
def export_cmd(min_events: int = typer.Option(1, help="Include users with events >= min_events"),
               dt: str = typer.Option(None, help="Partition date YYYY-MM-DD")):
    res = run_export_audience(min_events=min_events, dt=dt)
    print(f"Export complete export_id={res['export_id']} run_id={res['run_id']}")
    print(f"output: {res['output_path']} ({res['rows']} rows)")
    print(f"evidence: {res['evidence']}")

gdpr_app = typer.Typer()
app.add_typer(gdpr_app, name="gdpr")

@gdpr_app.command("request")
def gdpr_request_cmd(user_id: str = typer.Option(..., "--user-id"), mode: str = typer.Option("delete"), dt: str = typer.Option(None)):
    res = request_delete(user_id=user_id, mode=mode, dt=dt)
    print(f"GDPR request fulfilled: request_id={res.request_id} run_id={res.run_id}")
    print(f"evidence: {res.evidence_path}")

def main():
    app()

if __name__ == "__main__":
    main()
