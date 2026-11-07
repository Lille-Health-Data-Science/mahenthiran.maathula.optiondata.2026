"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = os.environ.get("FHIR_BASE", "https://hapi.fhir.org/baseR4")   # panne volontaire (TD.md, 4.4) : mettre une mauvaise URL
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []    
        while url:
            r = requests.get(
                url,
                params=requete,
                headers={"Accept": "application/fhir+json"},
                timeout=30
            )

            r.raise_for_status()
            bundle = r.json()

            for entry in bundle.get("entry", []):
                ressources.append(entry["resource"])

            url = next(
                (lien["url"] for lien in bundle.get("link", [])
                 if lien.get("relation") == "next"),
                None
            )

            requete = None

        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)      








        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None
        raise NotImplementedError("Étape 1 à compléter (ou déclencher avec source = fichier)")
        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Étape 2 : une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet."""
        import pandas as pd
        lignes = []

        # Lire le fichier NDJSON ligne par ligne
        with open(chemin, "r", encoding="utf-8") as f:
            for ligne in f:
                observation = json.loads(ligne)

                # Extraire les informations utiles
                lignes.append({
                    "id": observation.get("id"),
                    "patient": observation.get("subject", {}).get("reference"),
                    "date": observation.get("effectiveDateTime"),
                    "statut": observation.get("status"),
                    "valeur": observation.get("valueQuantity", {}).get("value"),
                    "unite": observation.get("valueQuantity", {}).get("unit")
                })

        # Construire un tableau Pandas
        df = pd.DataFrame(lignes)

        # Définir le chemin du fichier Parquet
        sortie = DONNEES / "aplaties" / (Path(chemin).stem + ".parquet")
        sortie.parent.mkdir(parents=True, exist_ok=True)

        # Enregistrer au format Parquet
        df.to_parquet(sortie, index=False)

        print(f"{len(df)} lignes enregistrées")
        print("Unités :", df["unite"].value_counts(dropna=False).to_dict())

        return str(sortie)

        # À COMPLÉTER (étape 2) : lire le NDJSON ligne par ligne (json.loads), construire un DataFrame,
        # l'écrire avec df.to_parquet(sortie, index=False) et renvoyer str(sortie)
        raise NotImplementedError("Étape 2 à compléter")

    @task
    def valider(chemin: str) -> str:
        """Étape 3 : vérifier les glycémies avec Pandera."""
        import pandas as pd
        import pandera.pandas as pa

        # Lire le fichier Parquet
        df = pd.read_parquet(chemin)

        # Définir les règles de validation
        schema = pa.DataFrameSchema({
            "patient": pa.Column(str, nullable=False),
            "valeur": pa.Column(float, checks=pa.Check.gt(0), nullable=False),
            "unite": pa.Column(str, checks=pa.Check.isin(UNITES), nullable=False)
        }, coerce=True)

        # Repérer les lignes valides
        masque = (
            df["patient"].notna()
            & df["valeur"].notna()
            & (df["valeur"] > 0)
            & df["unite"].isin(UNITES)
        )

        df_valides = df[masque].copy()
        df_rejetees = df[~masque].copy()

        # Vérifier les lignes conservées avec Pandera
        schema.validate(df_valides)

        # Enregistrer les données valides
        sortie = DONNEES / "valides" / Path(chemin).name
        sortie.parent.mkdir(parents=True, exist_ok=True)

        df_valides.to_parquet(sortie, index=False)

        print(f"Lignes valides : {len(df_valides)}")
        print(f"Lignes rejetées : {len(df_rejetees)}")

        return str(sortie)
    
    
    
    
    
    
    
    
    
    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : charger les glycémies dans DuckDB."""
        import duckdb

        # Chemin de la base de données
        base = DONNEES / "entrepot.duckdb"

        # Connexion à DuckDB
        con = duckdb.connect(str(base))

        # Création de la table si elle n'existe pas
        con.execute("""
            CREATE TABLE IF NOT EXISTS glycemies (
                id VARCHAR,
                patient VARCHAR,
                date TIMESTAMP,
                statut VARCHAR,
                valeur_mg_dl DOUBLE,
                unite VARCHAR,
                jour DATE
            )
        """)

        # Jour traité
        date_jour = jour(data_interval_start)

        # Charger le fichier Parquet
        df = con.execute(
            "SELECT * FROM read_parquet(?)", [chemin]
        ).df()

        # Conversion des glycémies en mg/dL
        df["valeur_mg_dl"] = df.apply(
            lambda ligne:
                ligne["valeur"] * 100 if ligne["unite"] == "g/L"
                else ligne["valeur"] * 18.0182 if ligne["unite"] == "mmol/L"
                else ligne["valeur"],
            axis=1
        )

        # Préparer les données
        df["jour"] = date_jour
        df["date"] = df["date"].pipe(
            lambda s: __import__("pandas").to_datetime(
                s, utc=True
            ).dt.tz_localize(None)
        )

        # Remplacer les données du jour (évite les doublons)
        try:
            con.execute("BEGIN TRANSACTION")

            con.execute(
                "DELETE FROM glycemies WHERE jour = ?",
                [date_jour]
            )

            con.register("nouvelles_glycemies", df)

            con.execute("""
                INSERT INTO glycemies
                SELECT
                    id, patient, date, statut,
                    valeur_mg_dl, unite, CAST(jour AS DATE)
                FROM nouvelles_glycemies
            """)

            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

        print(f"{len(df)} glycémies chargées dans DuckDB")

    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()
