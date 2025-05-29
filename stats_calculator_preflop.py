# stats_calculator_preflop.py
import sqlite3
# Supondo que PlayerStats e constantes relevantes (POSITION_CATEGORIES, etc.)
# estão no módulo principal 'stats_calculator' ou são passadas adequadamente.
# Para este arquivo, focamos nas queries.
# from .stats_calculator import POSITION_CATEGORIES, PF_POS_CATS_FOR_STATS, PF_POS_CATS_FOR_CALL_STATS

def calculate_preflop_stats_for_player(ps, cursor: sqlite3.Cursor, player_id: int):
    """
    Calcula e preenche as estatísticas Pré-Flop para o objeto PlayerStats (ps).
    ps.hands_played já deve estar preenchido.
    """
    if ps.hands_played == 0:
        return

    # --- VPIP ---
    ps.vpip_opportunities = ps.hands_played
    cursor.execute("""
        SELECT COUNT(DISTINCT main_a.hand_db_id)
        FROM actions main_a
        JOIN hand_players hp ON main_a.hand_db_id = hp.hand_db_id AND main_a.player_id = hp.player_id
        WHERE main_a.player_id = ? AND main_a.street = 'Preflop'
          AND main_a.action_type IN ('calls', 'bets', 'raises')
          AND NOT ( /* Exclui SB completando ou BB checkando sem raise anterior */
                (hp.position = 'SB' AND main_a.action_type = 'calls' AND
                 main_a.amount = (SELECT h.big_blind_amount * 0.5 FROM hands h WHERE h.hand_db_id = main_a.hand_db_id) AND
                 COALESCE((SELECT COUNT(*) FROM actions r WHERE r.hand_db_id = main_a.hand_db_id AND r.street = 'Preflop' AND r.action_type IN ('bets', 'raises') AND r.action_sequence < main_a.action_sequence), 0) = 0
                )
                OR
                (hp.position = 'BB' AND main_a.action_type = 'calls' AND main_a.amount = 0 AND
                 COALESCE((SELECT COUNT(*) FROM actions r WHERE r.hand_db_id = main_a.hand_db_id AND r.street = 'Preflop' AND r.action_type IN ('bets', 'raises') AND r.action_sequence < main_a.action_sequence), 0) = 0
                )
          )
    """, (player_id,))
    count_row = cursor.fetchone()
    ps.vpip_actions = count_row[0] if count_row and count_row[0] is not None else 0

    # --- PFR ---
    ps.pfr_opportunities = ps.hands_played
    cursor.execute("""
        SELECT COUNT(DISTINCT a.hand_db_id) FROM actions a
        WHERE a.player_id = ? AND a.street = 'Preflop' AND a.action_type IN ('bets', 'raises')
    """, (player_id,))
    count_row = cursor.fetchone()
    ps.pfr_actions = count_row[0] if count_row and count_row[0] is not None else 0

    # --- 3Bet PF ---
    cursor.execute("""
        WITH PFActionCounts AS (
            SELECT
                a.hand_db_id, a.player_id as actor_id, a.action_sequence, a.action_type,
                COALESCE((SELECT COUNT(*) FROM actions prev_a
                          WHERE prev_a.hand_db_id = a.hand_db_id AND prev_a.street = 'Preflop'
                            AND prev_a.action_type IN ('bets', 'raises')
                            AND prev_a.action_sequence < a.action_sequence), 0) as raises_before_this_action
            FROM actions a
            WHERE a.street = 'Preflop' AND a.player_id IS NOT NULL
        ),
        ThreeBetOpps AS (
            SELECT DISTINCT ac.hand_db_id
            FROM PFActionCounts ac
            WHERE ac.actor_id = ? AND ac.action_type NOT IN ('posts_sb', 'posts_bb', 'posts_ante', 'folds')
              AND ac.raises_before_this_action = 1
              AND NOT EXISTS (
                  SELECT 1 FROM actions r1
                  WHERE r1.hand_db_id = ac.hand_db_id AND r1.street = 'Preflop'
                    AND r1.action_type IN ('bets', 'raises') AND r1.player_id = ? /* O próprio jogador não foi o 1st raiser */
                    AND r1.action_sequence < ac.action_sequence
              )
        ),
        ThreeBetActs AS (
            SELECT DISTINCT ac.hand_db_id
            FROM PFActionCounts ac
            WHERE ac.actor_id = ? AND ac.action_type IN ('bets', 'raises')
              AND ac.raises_before_this_action = 1 /* Esta ação é o 2º raise */
        )
        SELECT (SELECT COUNT(*) FROM ThreeBetOpps) as opps, (SELECT COUNT(*) FROM ThreeBetActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.three_bet_pf_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.three_bet_pf_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Fold to PF 3Bet ---
    cursor.execute("""
        WITH PlayerOpenRaises AS (
            SELECT DISTINCT a.hand_db_id, a.action_sequence as or_seq
            FROM actions a
            WHERE a.player_id = ? AND a.street = 'Preflop' AND a.action_type IN ('bets', 'raises')
            AND COALESCE((SELECT COUNT(*) FROM actions pr_a WHERE pr_a.hand_db_id = a.hand_db_id AND pr_a.street = 'Preflop'
                 AND pr_a.action_type IN ('bets', 'raises') AND pr_a.action_sequence < a.action_sequence), 0) = 0
        ),
        Faced3BetAfterOR AS (
            SELECT DISTINCT por.hand_db_id, three_b.action_sequence as three_bet_seq
            FROM PlayerOpenRaises por
            JOIN actions three_b ON por.hand_db_id = three_b.hand_db_id
            WHERE three_b.street = 'Preflop' AND three_b.action_type IN ('bets', 'raises') AND three_b.player_id != ?
            AND COALESCE((SELECT COUNT(*) FROM actions pr_b WHERE pr_b.hand_db_id = three_b.hand_db_id AND pr_b.street = 'Preflop'
                 AND pr_b.action_type IN ('bets', 'raises') AND pr_b.action_sequence < three_b.action_sequence), 0) = 1
            AND three_b.action_sequence > por.or_seq
        ),
        FoldedTo3BetActs AS (
            SELECT DISTINCT f3b.hand_db_id
            FROM Faced3BetAfterOR f3b
            JOIN actions p_fold ON f3b.hand_db_id = p_fold.hand_db_id
            WHERE p_fold.player_id = ? AND p_fold.street = 'Preflop' AND p_fold.action_type = 'folds'
            AND p_fold.action_sequence > f3b.three_bet_seq
        )
        SELECT (SELECT COUNT(*) FROM Faced3BetAfterOR) as opps, (SELECT COUNT(*) FROM FoldedTo3BetActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.fold_to_pf_3bet_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.fold_to_pf_3bet_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Squeeze PF ---
    cursor.execute("""
        WITH OpenRaises AS (
            SELECT hand_db_id, player_id as or_player_id, action_sequence as or_seq
            FROM actions
            WHERE street = 'Preflop' AND action_type IN ('bets', 'raises')
            AND COALESCE((SELECT COUNT(*) FROM actions prev_r WHERE prev_r.hand_db_id = actions.hand_db_id AND prev_r.street = 'Preflop'
                 AND prev_r.action_type IN ('bets', 'raises') AND prev_r.action_sequence < actions.action_sequence), 0) = 0
        ),
        CallsAfterOpenRaise AS (
            SELECT ora.hand_db_id, ora.or_player_id, ora.or_seq, c.player_id as caller_id, c.action_sequence as call_seq
            FROM OpenRaises ora
            JOIN actions c ON ora.hand_db_id = c.hand_db_id
            WHERE c.street = 'Preflop' AND c.action_type = 'calls'
              AND c.player_id != ora.or_player_id AND c.action_sequence > ora.or_seq
              AND c.action_sequence < COALESCE(
                  (SELECT MIN(next_raise.action_sequence) FROM actions next_raise
                   WHERE next_raise.hand_db_id = ora.hand_db_id AND next_raise.street = 'Preflop'
                     AND next_raise.action_type IN ('bets', 'raises') AND next_raise.action_sequence > ora.or_seq),
                  999999
              )
        ),
        SqueezeSituations AS ( -- Identifica mãos com OR e pelo menos um caller
            SELECT hand_db_id, or_player_id, or_seq, MAX(call_seq) as last_caller_seq
            FROM CallsAfterOpenRaise
            GROUP BY hand_db_id, or_player_id, or_seq
            HAVING COUNT(DISTINCT caller_id) >= 1
        ),
        SqueezeOpps AS (
            SELECT DISTINCT ss.hand_db_id
            FROM SqueezeSituations ss
            JOIN actions player_act ON ss.hand_db_id = player_act.hand_db_id
            WHERE player_act.player_id = ?
              AND player_act.street = 'Preflop'
              AND player_act.action_type NOT IN ('posts_sb', 'posts_bb', 'posts_ante', 'folds')
              AND player_act.player_id != ss.or_player_id
              AND NOT EXISTS ( -- Jogador não é um dos callers
                  SELECT 1 FROM CallsAfterOpenRaise caor_check
                  WHERE caor_check.hand_db_id = ss.hand_db_id AND caor_check.caller_id = ?
                    AND caor_check.call_seq < player_act.action_sequence
              )
              AND player_act.action_sequence > ss.last_caller_seq -- Jogador age depois do último caller
              AND COALESCE((SELECT COUNT(*) FROM actions r_before_sq_opp 
                            WHERE r_before_sq_opp.hand_db_id = ss.hand_db_id AND r_before_sq_opp.street = 'Preflop'
                              AND r_before_sq_opp.action_type IN ('bets','raises') 
                              AND r_before_sq_opp.action_sequence < player_act.action_sequence), 0) = 1 -- Apenas o OR original antes da opp de squeeze
        ),
        SqueezeActs AS (
            SELECT DISTINCT so.hand_db_id
            FROM SqueezeOpps so
            JOIN actions sq_act ON so.hand_db_id = sq_act.hand_db_id
            WHERE sq_act.player_id = ? AND sq_act.street = 'Preflop' AND sq_act.action_type IN ('bets', 'raises')
        )
        SELECT (SELECT COUNT(*) FROM SqueezeOpps) as opps, (SELECT COUNT(*) FROM SqueezeActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.squeeze_pf_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.squeeze_pf_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- 4Bet PF ---
    cursor.execute("""
        WITH PFActionCounts AS (
            SELECT a.hand_db_id, a.player_id as actor_id, a.action_sequence, a.action_type,
                   COALESCE((SELECT COUNT(*) FROM actions prev_a WHERE prev_a.hand_db_id = a.hand_db_id AND prev_a.street = 'Preflop' AND prev_a.action_type IN ('bets', 'raises') AND prev_a.action_sequence < a.action_sequence), 0) as raises_before_this_action
            FROM actions a WHERE a.street = 'Preflop' AND a.player_id IS NOT NULL
        ),
        FourBetOpps AS (
            SELECT DISTINCT ac.hand_db_id
            FROM PFActionCounts ac
            WHERE ac.actor_id = ? AND ac.action_type NOT IN ('posts_sb', 'posts_bb', 'posts_ante', 'folds')
              AND ac.raises_before_this_action = 2
              AND NOT EXISTS (SELECT 1 FROM actions r_prev WHERE r_prev.hand_db_id = ac.hand_db_id AND r_prev.street = 'Preflop' AND r_prev.action_type IN ('bets', 'raises') AND r_prev.player_id = ? AND r_prev.action_sequence < ac.action_sequence)
        ),
        FourBetActs AS (
            SELECT DISTINCT ac.hand_db_id
            FROM PFActionCounts ac
            WHERE ac.actor_id = ? AND ac.action_type IN ('bets', 'raises')
              AND ac.raises_before_this_action = 2
        )
        SELECT (SELECT COUNT(*) FROM FourBetOpps) as opps, (SELECT COUNT(*) FROM FourBetActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.four_bet_pf_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.four_bet_pf_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Fold to PF 4Bet ---
    cursor.execute("""
        WITH Player3Bets AS (
            SELECT DISTINCT a.hand_db_id, a.action_sequence as three_bet_seq
            FROM actions a
            WHERE a.player_id = ? AND a.street = 'Preflop' AND a.action_type IN ('bets', 'raises')
            AND COALESCE((SELECT COUNT(*) FROM actions pr_a WHERE pr_a.hand_db_id = a.hand_db_id AND pr_a.street = 'Preflop'
                 AND pr_a.action_type IN ('bets', 'raises') AND pr_a.action_sequence < a.action_sequence), 0) = 1
        ),
        Faced4BetAfter3Bet AS (
            SELECT DISTINCT p3b.hand_db_id, four_b.action_sequence as four_bet_seq
            FROM Player3Bets p3b
            JOIN actions four_b ON p3b.hand_db_id = four_b.hand_db_id
            WHERE four_b.street = 'Preflop' AND four_b.action_type IN ('bets', 'raises') AND four_b.player_id != ?
            AND COALESCE((SELECT COUNT(*) FROM actions pr_b WHERE pr_b.hand_db_id = four_b.hand_db_id AND pr_b.street = 'Preflop'
                 AND pr_b.action_type IN ('bets', 'raises') AND pr_b.action_sequence < four_b.action_sequence), 0) = 2
            AND four_b.action_sequence > p3b.three_bet_seq
        ),
        FoldedTo4BetActs AS (
            SELECT DISTINCT f4b.hand_db_id
            FROM Faced4BetAfter3Bet f4b
            JOIN actions p_fold ON f4b.hand_db_id = p_fold.hand_db_id
            WHERE p_fold.player_id = ? AND p_fold.street = 'Preflop' AND p_fold.action_type = 'folds'
            AND p_fold.action_sequence > f4b.four_bet_seq
        )
        SELECT (SELECT COUNT(*) FROM Faced4BetAfter3Bet) as opps, (SELECT COUNT(*) FROM FoldedTo4BetActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.fold_to_pf_4bet_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.fold_to_pf_4bet_actions = res['acts'] if res and res['acts'] is not None else 0
    
    # --- Fold BB vs Steal (BTN, CO, SB) ---
    for steal_pos_key in ['BTN', 'CO', 'SB']:
        cursor.execute(f"""
            WITH StealAttempt AS (
                SELECT DISTINCT h.hand_db_id, steal_attempt.action_sequence as steal_seq
                FROM hands h
                JOIN hand_players hp_bb ON h.hand_db_id = hp_bb.hand_db_id AND hp_bb.player_id = ? AND hp_bb.position = 'BB'
                JOIN hand_players hp_stealer ON h.hand_db_id = hp_stealer.hand_db_id AND hp_stealer.position = ?
                JOIN actions steal_attempt ON h.hand_db_id = steal_attempt.hand_db_id AND steal_attempt.player_id = hp_stealer.player_id
                WHERE steal_attempt.street = 'Preflop' AND steal_attempt.action_type IN ('bets', 'raises')
                  AND COALESCE((SELECT COUNT(*) FROM actions prev_r 
                               WHERE prev_r.hand_db_id = h.hand_db_id AND prev_r.street = 'Preflop' 
                                 AND prev_r.action_type IN ('bets','raises') AND prev_r.action_sequence < steal_attempt.action_sequence), 0) = 0
            ),
            BBOpportunityToReact AS (
                SELECT DISTINCT sa.hand_db_id
                FROM StealAttempt sa
                WHERE EXISTS ( 
                    SELECT 1 FROM actions bb_action_opp
                    WHERE bb_action_opp.hand_db_id = sa.hand_db_id AND bb_action_opp.player_id = ? AND bb_action_opp.street = 'Preflop'
                      AND bb_action_opp.action_type NOT IN ('posts_bb')
                      AND bb_action_opp.action_sequence > sa.steal_seq
                )
            ),
            BBFoldedToSteal AS (
                SELECT DISTINCT bbor.hand_db_id
                FROM BBOpportunityToReact bbor
                JOIN actions bb_fold_act ON bbor.hand_db_id = bb_fold_act.hand_db_id
                JOIN StealAttempt sa_check ON bbor.hand_db_id = sa_check.hand_db_id -- Para re-usar steal_seq
                WHERE bb_fold_act.player_id = ? AND bb_fold_act.street = 'Preflop' AND bb_fold_act.action_type = 'folds'
                  AND bb_fold_act.action_sequence > sa_check.steal_seq
            )
            SELECT (SELECT COUNT(*) FROM BBOpportunityToReact) as opps, (SELECT COUNT(*) FROM BBFoldedToSteal) as acts
        """, (player_id, steal_pos_key, player_id, player_id))
        res_steal = cursor.fetchone()
        opps = res_steal['opps'] if res_steal and res_steal['opps'] is not None else 0
        acts = res_steal['acts'] if res_steal and res_steal['acts'] is not None else 0

        if steal_pos_key == 'BTN':
            ps.fold_bb_vs_btn_steal_opportunities = opps
            ps.fold_bb_vs_btn_steal_actions = acts
        elif steal_pos_key == 'CO':
            ps.fold_bb_vs_co_steal_opportunities = opps
            ps.fold_bb_vs_co_steal_actions = acts
        elif steal_pos_key == 'SB':
            ps.fold_bb_vs_sb_steal_opportunities = opps
            ps.fold_bb_vs_sb_steal_actions = acts

    # --- Open Raise por Posição Categórica (EP, MP, CO, BTN, SB) ---
    # Importa diretamente o modulo principal para obter as constantes necessarias.
    # Usar import absoluto evita erros de "relative import" quando o arquivo e executado
    # fora de um pacote Python propriamente definido.
    from stats_calculator import POSITION_CATEGORIES, PF_POS_CATS_FOR_STATS
    for pos_cat_key in PF_POS_CATS_FOR_STATS:
        actual_positions_in_cat = [p for p, cat in POSITION_CATEGORIES.items() if cat == pos_cat_key]
        if not actual_positions_in_cat: continue
        placeholders = ','.join(['?'] * len(actual_positions_in_cat))
        
        # Parâmetros para a query: player_id (para PlayerInCategoricalPosition), [lista de posições], player_id (para IsFirst...), player_id (para IsFirst...), player_id (para OpenRaiseActs), player_id (para OpenRaiseActs)
        params = [player_id] + actual_positions_in_cat + [player_id, player_id, player_id, player_id]
        
        query_or_pos = f"""
            WITH PlayerInCategoricalPosition AS (
                SELECT DISTINCT hp.hand_db_id FROM hand_players hp WHERE hp.player_id = ? AND hp.position IN ({placeholders})
            ),
            IsFirstToActVoluntarilyCat AS (
                SELECT picp.hand_db_id FROM PlayerInCategoricalPosition picp
                WHERE NOT EXISTS (
                    SELECT 1 FROM actions prev_a 
                    WHERE prev_a.hand_db_id = picp.hand_db_id AND prev_a.street = 'Preflop'
                      AND prev_a.action_type IN ('calls', 'bets', 'raises') 
                      AND prev_a.player_id != ? /* Ação de outro jogador */
                      AND prev_a.action_sequence < COALESCE(
                          (SELECT MIN(pa.action_sequence) FROM actions pa 
                           WHERE pa.hand_db_id = picp.hand_db_id AND pa.player_id = ? 
                             AND pa.street = 'Preflop' AND pa.action_type NOT IN ('posts_sb','posts_bb','posts_ante')), 
                          999999 /* Valor alto se jogador não agiu voluntariamente */
                      )
                )
            ),
            OpenRaiseActsCat AS (
                SELECT DISTINCT iftavc.hand_db_id FROM IsFirstToActVoluntarilyCat iftavc
                JOIN actions ora_cat ON iftavc.hand_db_id = ora_cat.hand_db_id AND ora_cat.player_id = ?
                WHERE ora_cat.street = 'Preflop' AND ora_cat.action_type IN ('bets', 'raises')
                 AND ora_cat.action_sequence = COALESCE(
                       (SELECT MIN(pa_vol_cat.action_sequence) FROM actions pa_vol_cat 
                        WHERE pa_vol_cat.hand_db_id = iftavc.hand_db_id AND pa_vol_cat.player_id = ? 
                          AND pa_vol_cat.street = 'Preflop' AND pa_vol_cat.action_type NOT IN ('posts_sb','posts_bb','posts_ante')),
                       -1 /* Para não dar match se não houver ação voluntária */
                  )
            )
            SELECT (SELECT COUNT(DISTINCT hand_db_id) FROM IsFirstToActVoluntarilyCat) as opps,
                   (SELECT COUNT(DISTINCT hand_db_id) FROM OpenRaiseActsCat) as acts
        """
        cursor.execute(query_or_pos, params)
        res_or_cat = cursor.fetchone()
        if res_or_cat:
            setattr(ps, f"open_raise_{pos_cat_key.lower()}_opportunities", res_or_cat['opps'] or 0)
            setattr(ps, f"open_raise_{pos_cat_key.lower()}_actions", res_or_cat['acts'] or 0)
            
    # --- Call Open Raise por Posição Categórica ---
    # Utiliza import absoluto para evitar problemas de importacao relativa quando
    # o projeto nao estiver configurado como pacote.
    from stats_calculator import PF_POS_CATS_FOR_CALL_STATS
    for pos_cat_key in PF_POS_CATS_FOR_CALL_STATS:
        actual_positions_in_cat = [p for p, cat in POSITION_CATEGORIES.items() if cat == pos_cat_key]
        if not actual_positions_in_cat: continue
        placeholders = ','.join(['?'] * len(actual_positions_in_cat))
        
        # Ordem dos parametros deve corresponder aos marcadores "?" na query.
        # Sao necessarios apenas tres valores de player_id: para o OR adversario,
        # para a verificacao da acao do proprio jogador e para a acao de call.
        params_call_or = [player_id, player_id] + actual_positions_in_cat + [player_id]

        query_call_or = f"""
            WITH OpenRaisesByOthers AS (
                SELECT DISTINCT a.hand_db_id, a.player_id as or_player_id, a.action_sequence as or_seq
                FROM actions a
                WHERE a.street = 'Preflop' AND a.action_type IN ('bets', 'raises') AND a.player_id != ?
                AND COALESCE((SELECT COUNT(*) FROM actions prev_r WHERE prev_r.hand_db_id = a.hand_db_id AND prev_r.street = 'Preflop'
                     AND prev_r.action_type IN ('bets', 'raises') AND prev_r.action_sequence < a.action_sequence), 0) = 0
            ),
            PlayerCanCallOR AS (
                SELECT DISTINCT oro.hand_db_id
                FROM OpenRaisesByOthers oro
                JOIN hand_players hp ON oro.hand_db_id = hp.hand_db_id
                JOIN actions player_act_opp ON oro.hand_db_id = player_act_opp.hand_db_id AND player_act_opp.player_id = ?
                WHERE hp.player_id = player_act_opp.player_id AND hp.position IN ({placeholders})
                  AND player_act_opp.street = 'Preflop'
                  AND player_act_opp.action_type NOT IN ('posts_sb', 'posts_bb', 'posts_ante', 'folds')
                  AND player_act_opp.action_sequence > oro.or_seq
                  AND COALESCE((SELECT COUNT(*) FROM actions r_between WHERE r_between.hand_db_id = oro.hand_db_id AND r_between.street = 'Preflop'
                       AND r_between.action_type IN ('bets','raises') AND r_between.action_sequence > oro.or_seq AND r_between.action_sequence < player_act_opp.action_sequence), 0) = 0
            ),
            PlayerCalledOR AS (
                SELECT DISTINCT pcco.hand_db_id
                FROM PlayerCanCallOR pcco
                JOIN actions call_act ON pcco.hand_db_id = call_act.hand_db_id
                WHERE call_act.player_id = ? AND call_act.street = 'Preflop' AND call_act.action_type = 'calls'
            )
            SELECT (SELECT COUNT(DISTINCT hand_db_id) FROM PlayerCanCallOR) as opps,
                   (SELECT COUNT(DISTINCT hand_db_id) FROM PlayerCalledOR) as acts
        """
        cursor.execute(query_call_or, params_call_or)
        res_call_or = cursor.fetchone()
        if res_call_or:
            setattr(ps, f"call_open_raise_{pos_cat_key.lower()}_opportunities", res_call_or['opps'] or 0)
            setattr(ps, f"call_open_raise_{pos_cat_key.lower()}_actions", res_call_or['acts'] or 0)
