from swat import CAS
import pandas as pd
import numpy as np
from math import floor
import os

# After GW 29, there is a gap
# GW39 is actually 30 (-9)
# GW47 is actually 38 (-9)

replacements = [
    (i, i-9) for i in range(39, 48)
]

limits = [
    {'position': 'G', 'element_type': 1, 'squad': 2, 'lineup_min': 1, 'lineup_max': 1},
    {'position': 'D', 'element_type': 2, 'squad': 5, 'lineup_min': 3, 'lineup_max': 5},
    {'position': 'M', 'element_type': 3, 'squad': 5, 'lineup_min': 2, 'lineup_max': 5},
    {'position': 'F', 'element_type': 4, 'squad': 3, 'lineup_min': 1, 'lineup_max': 3}
]

def preprocess():
    print('Preprocess 1/5')
    scores_df = pd.read_excel('../data/2019hindsight.xlsx', sheet_name='merged_gw')
    
    gw28_data = pd.read_csv('../data/gw28_players_raw.csv')
    gw28_data['name'] = gw28_data['first_name'] + '_' + gw28_data['second_name'] + '_' + gw28_data['id'].astype(str)
    gw28_data = gw28_data[['name', 'event_points', 'now_cost']].copy().assign(GW=28)
    merged = pd.merge(scores_df, gw28_data, on=['name', 'GW'], how='outer')
    #merged['total_points'].fillna(merged['event_points'], inplace=True)
    merged['total_points'].fillna(0, inplace=True)
    merged['value'].fillna(merged['now_cost'], inplace=True)
    merged.drop(columns=['event_points', 'now_cost'], inplace=True)
    # merged.drop

    gw18_data = pd.read_csv('../data/gw18_players_raw.csv')
    gw18_data['name'] = gw18_data['first_name'] + '_' + gw18_data['second_name'] + '_' + gw18_data['id'].astype(str)
    gw18_data = gw18_data[['name', 'event_points', 'now_cost']].copy().assign(GW=18)
    merged = pd.merge(merged, gw18_data, on=['name', 'GW'], how='outer')
    # merged['total_points'].fillna(merged['event_points'], inplace=True)
    merged['total_points'].fillna(0, inplace=True)
    merged['value'].fillna(merged['now_cost'], inplace=True)
    merged.drop(columns=['event_points', 'now_cost'], inplace=True)

    merged.to_csv('../data/cleaned_all_gw.csv')

    scores_df = merged.copy()
    scores_df['id'] = scores_df['name'].str.split('_', expand=True)[2].astype(int)
    scores_df['GW'] = scores_df['GW'].replace(dict(replacements))

    score_regular = scores_df[scores_df['GW']!=38].copy().reset_index(drop=True)
    score_lastwk  = scores_df[scores_df['GW']==38].copy().reset_index(drop=True)
    score_lastwk.drop_duplicates(inplace=True)
    scores_df = pd.concat([score_regular, score_lastwk]).reset_index()
    all_gameweeks = list(range(1,39))
    scores_df_sum =  scores_df.groupby(['id', 'GW', 'value', 'name'], as_index=False)['total_points'].sum()
    # player_start_gw = scores_df_sum.groupby(['id'])['GW'].min().to_dict()
    # all_combs = pd.DataFrame(list((i,g) for i in scores_df_sum['id'].unique() for g in all_gameweeks if player_start_gw[i] <= g), columns=['id', 'GW'])

    print('Preprocess 2/5')

    price_dict = scores_df_sum.groupby(['id', 'GW']).min()['value'].to_dict()
    copy_df = scores_df_sum.copy()
    def find_best_buy_sell_price(r):
        if (r['id'], r['GW']-1) in price_dict:
            r['buy_value'] = min(r['value'], price_dict[r['id'], r['GW']-1])
            r['sell_value'] = max(r['value'], price_dict[r['id'], r['GW']-1])
        else:
            r['buy_value'] = r['value']
            r['sell_value'] = r['value']
        return r
    scores_df_sum = scores_df_sum.apply(find_best_buy_sell_price, axis=1)

    print('Preprocess 3/5')

    price_csv = '../data/generated_price_info.csv'
    if os.path.exists(price_csv):
        price_combined = pd.read_csv(price_csv)
    else:
        price_df = scores_df_sum[['id', 'GW', 'buy_value', 'sell_value']]
        price_combined = pd.merge(price_df.assign(key = 0), price_df.assign(key=0), on=['id', 'key']).drop(columns=['sell_value_x', 'buy_value_y', 'key'])
        price_combined = price_combined[price_combined['GW_x'] < price_combined['GW_y']].copy()
        price_combined['actual_sell_price'] = price_combined['sell_value_y']
        filtered = price_combined['buy_value_x'] < price_combined['sell_value_y']
        price_combined.loc[filtered, 'actual_sell_price'] = np.floor((price_combined[filtered]['sell_value_y'] + price_combined[filtered]['buy_value_x'])/2).astype(int)
        price_combined.to_csv(price_csv)

    print('Preprocess 4/5')

    # scores_df_sum = scores_df_sum.apply(find_sell_price, axis=1)

    element_df = pd.read_csv('../data/players_raw.csv')
    element_df = element_df[['id', 'team', 'element_type', 'web_name']].copy()
    print(element_df.head())
    limits_df = pd.DataFrame(limits)
    print(limits_df.head())

    print('Preprocess 5/5')

    initial_solution = pd.read_csv('../data/init_solution.csv')

    return {'points': scores_df_sum, 'elements': element_df, 'limits': limits_df, 'price': price_combined, 'initial_solution': initial_solution}

def solve_seasson():
    # upload data scores, elements, limits

    data = preprocess()

    elements = data['elements']
    points = data['points']
    element_types = data['limits']
    price = data['price']
    initial = data['initial_solution']
    teams = elements.groupby(by=['team'], as_index=False).first()[['team']].copy()
    gameweeks = points.groupby(by=['GW'], as_index=False).first()[['GW']].copy()

    elements.to_csv('../temp/element.csv')
    points.to_csv('../temp/points.csv')
    element_types.to_csv('../temp/element_types.csv')
    teams.to_csv('../temp/teams.csv')
    gameweeks.to_csv('../temp/gameweeks.csv')

    session = CAS(CAS_SERVER, CAS_PORT, CAS_USERNAME, CAS_PASSWORD)
    session.upload_frame(elements, casout={'name': 'element', 'replace': True})
    session.upload_frame(teams, casout={'name': 'team', 'replace': True})
    session.upload_frame(gameweeks, casout={'name': 'gameweek', 'replace': True})
    session.upload_frame(element_types, casout={'name': 'element_type', 'replace': True})
    session.upload_frame(points, casout={'name': 'element_gameweek', 'replace': True})
    session.upload_frame(price, casout={'name': 'price', 'replace': True})
    session.upload_frame(initial, casout={'name': 'initial_sol', 'replace': True})

    # optmodel codes are omitted
    optmodel_code = r"""
        /* SETS */
        set ELEMENT;
        set TEAM;
        set GAMEWEEK;
        set ELEMENT_TYPE;
        set <num, num> ELEMENT_GAMEWEEK;
        set <num, num, num> ELEMENT_BUYGW_SELLGW;
        set <num, num> CURRENT_SOL_SET;

        /* PARAMETERS */
        str elementName {ELEMENT};
        num elementType {ELEMENT};
        num elementTeam {ELEMENT};
        num buyPrice {ELEMENT, GAMEWEEK} init 0;
        num sellPrice {ELEMENT, GAMEWEEK} init 0;
        num actualSell {ELEMENT_BUYGW_SELLGW};
        str typeName {ELEMENT_TYPE};
        num typeInSquad {ELEMENT_TYPE};
        num minTypeLineup {ELEMENT_TYPE};
        num maxTypeLineup {ELEMENT_TYPE};
        num points {ELEMENT, GAMEWEEK} init 0;
        num initSquad {ELEMENT, GAMEWEEK} init 0;
        num initSquadFH {ELEMENT, GAMEWEEK} init 0;
        num initLineup {ELEMENT, GAMEWEEK} init 0;
        num initBench {ELEMENT, GAMEWEEK} init 0;
        num initCaptain {ELEMENT, GAMEWEEK} init 0;
        num initTransferIn {ELEMENT, GAMEWEEK} init 0;
        num initTransferOut {ELEMENT, GAMEWEEK} init 0;
        num initTripleCaptain {ELEMENT, GAMEWEEK} init 0;
        num initWildcard {GAMEWEEK} init 0;
        num initFreeHit {GAMEWEEK} init 0;
        num initBenchBoost {GAMEWEEK} init 0;
        num initBudgetInBank {GAMEWEEK} init 0;
        num initAvailableTransfers {GAMEWEEK} init 0;
        num initPenalizedTransfers {GAMEWEEK} init 0;


        /* CONSTANTS */
        num squadTotal = 15;
        num perTeamLimit = 3;
        num freeTransferPerWeek = 1;
        num freeTransferLimit = 2;
        num lineupLimit = 11;
        num benchLimit = 4;
        num goalkeeperType = 1;
        num beta = 13;
        num penaltyTransferPoints = 4;
        num initialBudget = 1000;

        /* READ DATA */
        read data element into ELEMENT=[id] elementName=web_name elementType=element_type elementTeam=team;
        read data team into TEAM=[team];
        read data gameweek into GAMEWEEK=[GW];
        read data element_type into ELEMENT_TYPE=[element_type] typeName=position typeInSquad=squad minTypeLineup=lineup_min maxTypeLineup=lineup_max;
        read data element_gameweek into ELEMENT_GAMEWEEK=[id GW] points=total_points buyPrice=buy_value sellPrice=sell_value;
        read data price into ELEMENT_BUYGW_SELLGW=[id GW_X GW_Y] actualSell=actual_sell_price;
        read data initial_sol into CURRENT_SOL_SET=[player_id gw_no]
            initSquad=Squad
            initSquadFH=SquadFreeHit
            initLineup=Lineup
            initBench=Bench
            initCaptain=Captain
            initTransferIn=TransferIn
            initTransferOut=TransferOut
            initTripleCaptain=TripleCaptain;
        read data initial_sol into [gw_no]
            initWildcard=Wildcard
            initFreeHit=FreeHit
            initBenchBoost=BenchBoost
            initBudgetInBank=BudgetInBank
            initAvailableTransfers=AvailableTransfers
            initPenalizedTransfers=PenalizedTransfers;
        
        /* VARIABLES */
        var Squad {ELEMENT, GAMEWEEK} binary;
        var SquadFreeHit {ELEMENT, GAMEWEEK} binary;
        var Lineup {ELEMENT, GAMEWEEK} binary;
        var Captain {ELEMENT, GAMEWEEK} binary;
        var Bench {ELEMENT, GAMEWEEK} binary;
        var TransferIn {ELEMENT, GAMEWEEK} binary;
        var TransferOut {ELEMENT, GAMEWEEK} binary;
        var WildCard {GAMEWEEK} binary;
        var TripleCaptain {ELEMENT, GAMEWEEK} binary;
        var BenchBoost {GAMEWEEK} binary;
        var FreeHit {GAMEWEEK} binary;
        var Aux {ELEMENT, GAMEWEEK} binary;
        var BudgetInBank {{0} UNION GAMEWEEK} >= 0 integer;
        var AvailableTransfers {GAMEWEEK UNION {39}} >= 0 integer;
        var PenalizedTransfers {GAMEWEEK} >= 0 integer;
        var BuySellPlayer {e in ELEMENT, g1 in GAMEWEEK, g2 in GAMEWEEK: <e,g1,g2> in ELEMENT_BUYGW_SELLGW} binary;

        fix BudgetInBank[0]=1000;

        /* BGW fix */
        for {e in ELEMENT} do;
            for {g in GAMEWEEK: g>2} do;
                if buyPrice[e,g] EQ 0 AND buyPrice[e,g-1] NE 0 then do;
                    buyPrice[e,g] = buyPrice[e,g-1];
                    sellPrice[e,g] = sellPrice[e,g-1];
                end;
            end;
        end;
        /* Restrict unavailable */
        for {e in ELEMENT, g in GAMEWEEK} do;
            if buyPrice[e,g] EQ 0 then do;
                fix TransferIn[e,g]=0;
                fix TransferOut[e,g]=0;
                fix Squad[e,g] = 0;
                fix SquadFreeHit[e,g] = 0;
            end;
        end;

        num matchFound;
        matchFound = 0;
        num g2;

        /* Read initial */
        for {e in ELEMENT} do;
            for {<(e),g> in CURRENT_SOL_SET} do;
                Squad[e,g]=initSquad[e,g];
                SquadFreeHit[e,g]=initSquadFH[e,g];
                Lineup[e,g]=initLineup[e,g];
                Bench[e,g]=initBench[e,g];
                Captain[e,g]=initCaptain[e,g];
                TransferIn[e,g]=initTransferIn[e,g];
                TransferOut[e,g]=initTransferOut[e,g];
                if (initTransferOut[e,g] EQ 1) then do;
                    matchFound = 0;
                    g2 = g-1;
                    do until (matchFound=1 or g2=0);
                        if (initTransferIn[e,g2] EQ 1) then do;
                            BuySellPlayer[e,g2,g] = 1;
                            matchFound=1;
                        end;
                        g2=g2-1;
                    end;
                end;
                TripleCaptain[e,g]=initTripleCaptain[e,g];
                if (Squad[e,g] EQ 1 AND Lineup[e,g] EQ 1) then do;
                    Aux[e, g] = 1;
                end;
            end;
        end;
        for {g in GAMEWEEK} do;
            Wildcard[g]=initWildcard[g];
            FreeHit[g]=initFreeHit[g];
            BenchBoost[g]=initBenchBoost[g];
            BudgetInBank[g]=initBudgetInBank[g];
            AvailableTransfers[g]=initAvailableTransfers[g];
            PenalizedTransfers[g]=initPenalizedTransfers[g];
        end;
        AvailableTransfers[39] = 1;

        /* CONSTRAINTS */

        /* Squad Constraints (4.8-4.17) omitted */
        /* Lineup Constraints (4.18-4.26) omitted */
        /* Gamechip Constraints (4.2-4.7) */
        con FirstWildCardChipRule:
            sum{g in GAMEWEEK: g <= 19} WildCard[g] <= 1;
        con SecondWildCardChipRule:
            sum{g in GAMEWEEK: g > 19 and g NE 30} WildCard[g] <= 1;
        con ThirdWildCardChipRule:
            sum{g in GAMEWEEK: g EQ 30} WildCard[g] <= 1;
        con TripleCaptainChipRule:
            sum{e in ELEMENT, g in GAMEWEEK} TripleCaptain[e, g] <= 1;
        con BenchBoostChipRule:
            sum{g in GAMEWEEK} BenchBoost[g] <= 1;
        con FreeHitChipRule:
            sum{g in GAMEWEEK} FreeHit[g] <= 1;
        con OneChipPerWeek {g in GAMEWEEK}:
            WildCard[g] + sum{e in ELEMENT} TripleCaptain[e,g] + BenchBoost[g] + FreeHit[g] <= 1;
        
        /* Substitution Constraints (4.30-4.32) omitted */
        /* Budget Constraints (4.33-4.39) omitted partially */
        con FirstWeekBudget:
            BudgetInBank[1] = initialBudget - sum{e in ELEMENT} buyPrice[e,1] * (Squad[e,1] + SquadFreeHit[e,1]);
        con FirstWeekTransfer {e in ELEMENT}:
            Squad[e,1] = TransferIn[e,1];
        con IterativeBudget {g in GAMEWEEK: g > 1}:
            BudgetInBank[g-1] + sum{e in ELEMENT, g1 in GAMEWEEK: <e,g1,(g)> in ELEMENT_BUYGW_SELLGW} (actualSell[e, g1, g] * BuySellPlayer[e, g1, g]) - sum{e in ELEMENT} (buyPrice[e, g] * TransferIn[e, g]) = BudgetInBank[g];
        con FreeHitBudgetLimit {g in GAMEWEEK: g > 1}:
            sum{e in ELEMENT} (sellPrice[e,g] * Squad[e, g-1]) + BudgetInBank[g-1] >= sum{e in ELEMENT} (buyPrice[e,g] * SquadFreeHit[e, g]);
        con FreeHitSellLimit {g in GAMEWEEK: g > 1}:
            sum{e in ELEMENT} TransferOut[e, g] <= squadTotal * (1 - FreeHit[g]);
        con FreeHitBuyLimit {g in GAMEWEEK: g > 1}:
            sum{e in ELEMENT} TransferIn[e, g] <= squadTotal * (1 - FreeHit[g]);

        /* Availability Constraints */
        con IndividualAvailability {e in ELEMENT, g in GAMEWEEK: g > 1}:
            Squad[e, g-1] + TransferIn[e, g] - TransferOut[e, g] = Squad[e, g];
        con BuyOrSellLimit {e in ELEMENT, g in GAMEWEEK}:
            TransferIn[e, g] + TransferOut[e, g] <= 1;

        /* Transfer Constraints (4.40-4.44) omitted */
        /* Buy/Sell/GW consistency */
        con BuySellLogic1 {<e,g1,g2> in ELEMENT_BUYGW_SELLGW}:
            BuySellPlayer[e, g1, g2] <= TransferIn[e,g1];
        con BuySellLogic2 {<e,g1,g2> in ELEMENT_BUYGW_SELLGW}:
            BuySellPlayer[e, g1, g2] <= TransferOut[e,g2];
        con CanOnlySellOncePerBuy {e in ELEMENT, g in GAMEWEEK}:
            sum{<(e),(g),g2> in ELEMENT_BUYGW_SELLGW} BuySellPlayer[e, g, g2] <= 1;
        con CanOnlyBuyOncePerSell {e in ELEMENT, g in GAMEWEEK}:
            sum{<(e),g1,(g)> in ELEMENT_BUYGW_SELLGW} BuySellPlayer[e, g1, g] <= 1;
        con CanOnlySellIfBought {e in ELEMENT, g in GAMEWEEK}:
            sum{<(e),g1,(g)> in ELEMENT_BUYGW_SELLGW} BuySellPlayer[e,g1,g] = TransferOut[e,g];

        impvar Contribution {e in ELEMENT, g in GAMEWEEK} = points[e,g] * (LineUp[e,g] + Captain[e,g] + 2 * TripleCaptain[e,g]);

        impvar TotalRating = 
            sum {e in ELEMENT, g in GAMEWEEK} (points[e,g] * (LineUp[e,g] + Captain[e,g] + 2 * TripleCaptain[e,g]))
            - (penaltyTransferPoints * sum{g in GAMEWEEK} PenalizedTransfers[g]);

        con RestrictedInitial {g in GAMEWEEK: g>1}:
            sum {e in ELEMENT} (TransferIn[e,g] + TransferOut[e,g]) = 0;
        
        /* No Hit */
        con NoHits {g in GAMEWEEK}:
            PenalizedTransfers[g] = 0;
        
        /* Ghost Ship Constraints (DISABLED) */
        /* con ForceGhostShipSquad{e in ELEMENT, g in GAMEWEEK: g > 1}:
            Squad[e,g] = Squad[e,1];
        con ForceGhostShipLineup1{e in ELEMENT, g in GAMEWEEK: g > 1}:
            Lineup[e,g] >= Lineup[e,1] - BenchBoost[1];
        con ForceGhostShipLineup2{e in ELEMENT, g in GAMEWEEK: g > 2}:
            Lineup[e,g] = Lineup[e,2];
        con ForceGhostShipBench1{e in ELEMENT, g in GAMEWEEK: g > 1}:
            Bench[e,g] >= Bench[e,1] - BenchBoost[1];
        con ForceGhostShipBench2{e in ELEMENT, g in GAMEWEEK: g > 2}:
            Bench[e,g] = Bench[e,2];
        con ForceGhostShipCaptain{e in ELEMENT, g in GAMEWEEK: g > 1}:
            Captain[e,g] = Captain[e,1] + TripleCaptain[e,1];
        con ForceGhostShipNoChip:
            sum{e in ELEMENT, g in GAMEWEEK: g NE 1} TripleCaptain[e,g] + sum{g in GAMEWEEK: g NE 1} (WildCard[g] + FreeHit[g] + BenchBoost[g]) = 0; */

        impvar ActualSellPriceCalc {e in ELEMENT, g in GAMEWEEK} = sum{g1 in GAMEWEEK: <(e),g1,(g)> in ELEMENT_BUYGW_SELLGW} actualSell[e, g1, g] * BuySellPlayer[e, g1, g];

        /* Objective Function (4.1) */
        max FPLObjective = TotalRating;
        
        /* Initial solve for restricted problem */
        solve with milp / maxtime=600 relobjgap=1e-6;
        
        drop RestrictedInitial;
        drop NoHits;

        /* Final solve */
        solve with milp / primalin maxtime=64800 relobjgap=1e-6;
        create data optimal_season from [player_id gw_no eltype team_code]={
                    e in ELEMENT, g in GAMEWEEK, et in ELEMENT_TYPE, t in TEAM:
                    elementTeam[e] EQ t AND elementType[e] EQ et}
                    Name=elementName[e]
                    Team=t
                    ElemTypeName=typeName[et]
                    BuyPrice=buyPrice[e,g]
                    SellPrice=sellPrice[e,g]
                    ActualSellPrice=ActualSellPriceCalc[e,g]
                    Points=points[e, g]
                    Contribution=Contribution[e,g]
                    Squad=Squad[e,g]
                    SquadFreeHit=SquadFreeHit[e,g]
                    Lineup=Lineup[e,g]
                    Bench=Bench[e,g]
                    Captain=Captain[e,g]
                    TransferIn=TransferIn[e,g]
                    TransferOut=TransferOut[e,g]
                    TripleCaptain=TripleCaptain[e,g]
                    Wildcard=Wildcard[g]
                    FreeHit=FreeHit[g]
                    BenchBoost=BenchBoost[g]
                    BudgetInBank=BudgetInBank[g]
                    AvailableTransfers=AvailableTransfers[g]
                    PenalizedTransfers=PenalizedTransfers[g];
    """

    session.loadactionset("optimization")
    session.runOptmodel(optmodel_code)
    solution = session.CASTable("optimal_season").to_frame()
    solution = solution.round(
                {'Squad': 0, 'SquadFreeHit': 0, 'Lineup': 0, 'Captain': 0, 'TransferIn': 0, 'TransferOut': 0,
                 'TripleCaptain': 0, 'Wildcard': 0, 'FreeHit': 0, 'BenchBoost': 0, 'AvailableTransfers': 0, 'PenalizedTransfers': 0, 'Contribution': 0, 'ActualSellPrice': 0})
    solution['important'] = solution["Squad"] + solution["SquadFreeHit"] + \
                            solution["TransferIn"] + solution["TransferOut"]
    solution = solution[solution['important'] > 0.5].copy()
    solution = solution.sort_values(
                by=["gw_no", "Squad", "Lineup", "eltype", "player_id"],
                ascending=[True, False, False, True, True],
                ignore_index=True)

    solution.to_csv('../data/solution.csv')

    session.close()

if __name__ == '__main__':
    solve_seasson()

