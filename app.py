# app.py
from flask import Flask, request, jsonify, render_template
from collections import defaultdict
import sqlite3
import os # Para verificar se o DB existe ao iniciar o servidor

# Importar módulos do seu projeto
import db_manager         # Para get_db_connection, create_tables
import stats_calculator   # Para PlayerStats, calculate_stats_for_single_player

app = Flask(__name__, template_folder='html_templates')

# Cache simples no lado do servidor para estatísticas de jogadores já calculadas na sessão
# Chave: player_name, Valor: objeto PlayerStats já com os dados calculados
PLAYER_STATS_CACHE = {}
# Poderia usar um cache mais sofisticado como LRUCache se a memória se tornar um problema
# from cachetools import LRUCache
# PLAYER_STATS_CACHE = LRUCache(maxsize=100) # Cache para os 100 jogadores mais recentes


def get_player_stats_object_from_db_or_cache(player_name_to_fetch: str) -> stats_calculator.PlayerStats | None:
    """
    Obtém o objeto PlayerStats para um jogador.
    Primeiro tenta o cache, depois calcula do DB se necessário e armazena no cache.
    """
    if player_name_to_fetch in PLAYER_STATS_CACHE:
        print(f"Servidor: Retornando stats de '{player_name_to_fetch}' do cache do servidor.")
        return PLAYER_STATS_CACHE[player_name_to_fetch]

    print(f"Servidor: Calculando stats para '{player_name_to_fetch}' a partir do DB...")
    conn = None
    try:
        conn = db_manager.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT player_id FROM players WHERE player_name = ?", (player_name_to_fetch,))
        player_row = cursor.fetchone()

        if not player_row:
            print(f"Servidor: Jogador '{player_name_to_fetch}' não encontrado no DB.")
            return None

        player_id = player_row['player_id']
        
        # Chama a função principal de cálculo de stats para este jogador
        player_stat_obj = stats_calculator.calculate_stats_for_single_player(conn, player_id, player_name_to_fetch)
        
        if player_stat_obj:
            PLAYER_STATS_CACHE[player_name_to_fetch] = player_stat_obj # Adiciona ao cache
            print(f"Servidor: Stats para '{player_name_to_fetch}' calculadas e cacheadas.")
        return player_stat_obj

    except sqlite3.Error as e:
        print(f"Erro de banco de dados ao buscar/calcular stats para {player_name_to_fetch}: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado ao buscar/calcular stats para {player_name_to_fetch}: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if conn:
            conn.close()


@app.route('/')
def index():
    # Servir o template HTML principal (grid)
    return render_template('stats_grid_template.html')

@app.route('/summary') # Endpoint para a página de resumo, se você fizer uma
def summary_page():
    # Se você criar um summary_template.html, sirva-o aqui
    # return render_template('summary_template.html')
    # Por enquanto, um placeholder:
    return "Página de resumo dinâmica (a ser implementada com busca similar ao grid)"


@app.route('/player_stats')
def get_player_stats_route():
    player_name = request.args.get('name')
    if not player_name:
        return jsonify({"error": "Nome do jogador não fornecido"}), 400

    print(f"Servidor: Requisição recebida para stats do jogador: {player_name}")
    player_stat_object = get_player_stats_object_from_db_or_cache(player_name)

    if player_stat_object:
        # Converter o objeto PlayerStats para um dicionário para o JSON
        # usando o método to_dict_display() que você já tem.
        try:
            stats_dict_display = player_stat_object.to_dict_display()
            return jsonify({
                "player_name": player_stat_object.player_name,
                "stats": stats_dict_display
            })
        except Exception as e:
            print(f"Erro ao converter stats para display para {player_name}: {e}")
            return jsonify({"error": f"Erro interno ao processar stats para {player_name}"}), 500
    else:
        return jsonify({"message": f"Jogador '{player_name}' não encontrado ou sem dados para exibir."}), 404

if __name__ == '__main__':
    # Verificar e criar tabelas no DB se não existirem ao iniciar o servidor.
    # Isso é útil para o primeiro run ou se o DB for apagado.
    # Em um ambiente de produção, migrações de DB seriam uma abordagem mais robusta.
    db_file = db_manager.DB_NAME
    should_create_tables = not os.path.exists(db_file) or os.path.getsize(db_file) == 0

    conn_init = None
    try:
        conn_init = db_manager.get_db_connection()
        if should_create_tables:
            print(f"Arquivo de banco de dados '{db_file}' não encontrado ou vazio. Criando tabelas...")
            db_manager.create_tables(conn_init)
            print("Tabelas criadas (ou já existiam).")
        else:
            print(f"Usando banco de dados existente: '{db_file}'")
    except sqlite3.Error as e:
        print(f"Erro ao inicializar banco de dados: {e}")
    except Exception as e:
        print(f"Erro inesperado durante inicialização do DB: {e}")
    finally:
        if conn_init:
            conn_init.close()
    
    print("\n--- Servidor Flask ---")
    print("Execute o `main_processor.py` separadamente para popular o banco de dados com novas mãos.")
    print("Acesse a interface no navegador em http://127.0.0.1:5000/")
    print("Para parar o servidor, pressione CTRL+C neste terminal.\n")
    
    # host='0.0.0.0' torna o servidor acessível na sua rede local, não apenas localhost
    # use_reloader=False pode ser útil se você estiver tendo problemas com o reinício automático
    # e a conexão com o banco de dados sendo fechada/reaberta incorretamente durante o desenvolvimento.
    # Para desenvolvimento, debug=True e use_reloader=True (padrão com debug=True) é geralmente bom.
    app.run(debug=True, host='0.0.0.0', port=5000)