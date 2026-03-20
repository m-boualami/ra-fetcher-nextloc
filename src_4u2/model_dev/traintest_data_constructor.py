import json
import pandas as pd
import ast
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / "data"


def gen_lineup_lookuptable(event_df):
    L = {}
    for lineup, eid in zip(event_df.linup_aids.values, event_df.id):
        x = ast.literal_eval(lineup)
        if isinstance(x, int):
            L[eid] = [x]
        else:
            L[eid] = list(x)
    return L


def gen_lineup_lookuptable_promoters(event_df):
    L = {}
    for lineup, pid in zip(event_df.linup_aids.values, event_df.promoter_id):
        x = ast.literal_eval(lineup)
        if isinstance(x, int):
            L[pid] = [x]
        else:
            L[pid] = list(x)
    return L


def gen_aid2eid_lookuptable(L_lookup):
    aid_to_lineups = defaultdict(list) # temporally ordered ordered 
    for lineup, aids in L_lookup.items():
        for aid in aids:
            aid_to_lineups[aid].append(lineup)
    return aid_to_lineups


def main():
    # ------- 1. load + sort the event data ------- 
    event_data = pd.read_csv(DATA_DIR / "clean_tables/events", index_col=0).iloc[:, 1:]
    event_data = event_data.sort_values('start_time')
    event_data['date'] = pd.to_datetime(event_data['start_time']).dt.normalize()


    # ------- 2. train-test split + save -------
    mask_train = (event_data['date'] >= pd.Timestamp('2025-03-01')) & (event_data['date'] < pd.Timestamp('2025-11-01'))
    train = event_data[mask_train]
    mask_test = (event_data['date'] >= pd.Timestamp('2025-11-01')) & (event_data['date'] < pd.Timestamp('2025-11-15'))
    test = event_data[mask_test]
    print("n. train events:", len(train))
    print("n. test events:", len(test))
    train.to_csv(DATA_DIR / "model_training_data/event_train")
    test.to_csv(DATA_DIR / "model_training_data/event_test")


    # ------- 3. gen artist eval dataset -------
    L_test = gen_lineup_lookuptable(test.dropna(subset='linup_aids'))
    L_train = gen_lineup_lookuptable(train.dropna(subset='linup_aids'))
    aid2lineups_train = gen_aid2eid_lookuptable(L_train)
    aid2lineups_test = gen_aid2eid_lookuptable(L_test)
    aids_train_and_test = list(set(aid2lineups_train.keys()) & set(aid2lineups_test.keys()))
    p_regular = len(aids_train_and_test)/len(aid2lineups_test)
    print(f"p djs in train who played also in last 6 months: {p_regular}")
    
    artist_eval_data = {aid:{'train_eids':[], 'test_eids':[]} for aid in aids_train_and_test}
    for aid in aids_train_and_test:
        artist_eval_data[aid]['train_eids'].extend(aid2lineups_train[aid]) 
        artist_eval_data[aid]['test_eids'].extend(aid2lineups_test[aid]) 
   
    # save the data
    with open(DATA_DIR / 'model_training_data/artist_eval_data.json', 'w') as f:
        json.dump(artist_eval_data, f)


    # ------- 4. gen promoter eval dataset -------
    L_test = gen_lineup_lookuptable_promoters(test.dropna(subset=['promoter_id', 'linup_aids']))
    L_train = gen_lineup_lookuptable_promoters(train.dropna(subset=['promoter_id', 'linup_aids']))

    pid2lineups_train = gen_aid2eid_lookuptable(L_train)
    pid2lineups_test = gen_aid2eid_lookuptable(L_test)

    # filter those with < 2 events
    pid2lineups_train = {pid:events for pid, events in pid2lineups_train.items() if len(events) > 1}
    pid2lineups_test = {pid:events for pid, events in pid2lineups_test.items() if len(events) > 1}
    # save the data
    with open(DATA_DIR / 'model_training_data/promoter_events_eval_train.json', 'w') as f:
        json.dump(pid2lineups_train, f)
    with open(DATA_DIR / 'model_training_data/promoter_events_eval_test.json', 'w') as f:
        json.dump(pid2lineups_test, f)

    
if __name__ == "__main__":
    main()