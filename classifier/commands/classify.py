"""
Classify loops through all the ads and save the scores to the database.
"""
import click
import dill
from classifier.utilities import (classifier_path, get_vectorizer,
                                  confs, DB, get_text)
import csv

import json
import requests
from os import environ

@click.command("classify")
@click.option("--newest/--every",
              default=True,
              help="Classify all of the records")
@click.option("--lang", help="Limit to language")
@click.pass_context
def classify(ctx, newest, lang):
    """
    Classify the ads in the database at $DATABASE_URL.
    """
    classifiers = dict()
    for (directory, conf) in confs(ctx.obj["base"]):
        if lang and conf["language"] != lang:
            continue
        with open(classifier_path(directory), 'rb') as classy:
            classifiers[conf["language"]] = {
                "classifier": dill.load(classy),
                "vectorizer": get_vectorizer(conf)
            }

    if newest:
        print("Running newest")
        query = "select * from fbpac_ads where political_probability = 0"
        if lang:
            query = query + " and lang = '{}'".format(lang)
        else:
            langs = map(lambda x: "'{}'".format(x), classifiers.keys())
            langs = ','.join(langs)

            query = query + " and lang in ({})".format(langs)
    else:
        print("Running every")
        query = "select * from fbpac_ads"
        if lang:
            query = query + " where lang = '{}'".format(lang)

    total = "select count(*) as length from ({}) as t1;"
    length = DB.query(total.format(query))[0]["length"]
    records = DB.query(query)
    print("found {} ads".format(length))
    updates = []
    query = "update fbpac_ads set political_probability=:probability where id=:id"
    idx = 0
    political_count = 0

    for record in records:
        idx += 1
        record_lang = "en-US" if record["lang"] == "en-IE" else record["lang"]
        if record_lang in classifiers:
            classifier = classifiers[record_lang]
            text = classifier["vectorizer"].transform([get_text(record)])
            probability = classifier["classifier"].predict_proba(text)[0][1]

            if probability > 0.70:
                political_count += 1

            update = {
                "id": record["id"],
                "probability": probability
            }
            if record["political_probability"] > update["probability"] and record["political_probability"] >= 0.70 and update["probability"] < 0.70 and not record["suppressed"]:
                print("refusing to downgrade probability of ad {}".format(record["id"]))

            updates.append(update)
            # out = "Classified {pid[id]} ({info[idx]} of {info[length]}) with {pid[probability]}"
            # print(out.format(pid=update, info={"length": length, "idx": idx}))

            if len(updates) >= 100:
                DB.bulk_query(query, updates)
                updates = []

    if updates:
        DB.bulk_query(query, updates)
    requests.post(environ.get("SLACKWH", 'example.com'), data=json.dumps({"text": f"(1/6): classified {idx} ads, of which {political_count} were political"}), headers={"Content-Type": "application/json"})
