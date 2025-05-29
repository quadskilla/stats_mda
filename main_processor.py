# main_processor.py
import os
from collections import defaultdict # Apenas se usado para algo antes de passar para stats_calculator

# Importar dos novos módulos
import db_manager
import hand_parser
# stats_calculator não é mais chamado diretamente aqui para calcular tudo
# html_generator não é mais chamado aqui

def process_log_files(log_content, conn): # Removido existing_processed_ids
    """
    Parsea o conteúdo do log e salva mãos novas no DB.
    Retorna a contagem de mãos novas inseridas.
    """
    hand_texts = []
    current_block = []
    for line in log_content.strip().split('\n'):
        if line.startswith("PokerStars Hand #") and current_block:
            hand_texts.append("\n".join(current_block))
            current_block = [line]
        elif line.strip() or current_block: # Mantém linhas em branco dentro de um bloco
            current_block.append(line)
    if current_block:
        hand_texts.append("\n".join(current_block))

    newly_inserted_db_count = 0

    if not hand_texts:
        return newly_inserted_db_count

    print(f"Analisando {len(hand_texts)} blocos de mão para inserção no DB...")
    for i, text_block in enumerate(hand_texts):
        header_match = hand_parser.RE_HAND_HEADER.match(text_block.split('\n')[0])
        if not header_match:
            continue
        
        hand_history_id = header_match.group(1)

        if not db_manager.check_hand_exists(conn, hand_history_id):
            hand_obj = hand_parser.parse_hand_history_to_object(text_block)
            if hand_obj:
                db_id = db_manager.save_hand_to_db(conn, hand_obj) # save_hand_to_db faz o commit internamente ou o chamador faz
                if db_id:
                    newly_inserted_db_count += 1
        
        if (i + 1) % 200 == 0:
            conn.commit() # Commit em lotes
            print(f"  Processadas {i+1}/{len(hand_texts)} mãos para o DB. {newly_inserted_db_count} novas inseridas.")
    
    conn.commit() # Commit final
    return newly_inserted_db_count


def main():
    input_filename = "historico_maos.txt"
    general_dir = "maos_gerais"

    conn = db_manager.get_db_connection()
    # create_tables é chamado agora pelo app.py ao iniciar, mas pode ser chamado aqui também se rodar este script como standalone para popular o DB.
    # db_manager.create_tables(conn) # Garante que tabelas existem

    log_parts = []
    if os.path.isfile(input_filename):
        try:
            with open(input_filename, "r", encoding="utf-8") as f: log_parts.append(f.read())
        except Exception as e: print(f"Erro ao ler '{input_filename}': {e}")
    else: print(f"Arquivo '{input_filename}' não encontrado.")
    
    if os.path.isdir(general_dir):
        for root, _, files in os.walk(general_dir):
            for fname in files:
                if fname.lower().endswith(".txt"):
                    fpath = os.path.join(root, fname)
                    try:
                        with open(fpath, "r", encoding="utf-8") as f: log_parts.append(f.read())
                    except Exception as e: print(f"Erro ao ler '{fpath}': {e}")
    
    if not log_parts:
        print("Nenhum arquivo de log encontrado para processar.")
        conn.close()
        return
    
    full_log_content = "\n\n".join(log_parts)
    if not full_log_content.strip():
        print("Conteúdo dos logs está vazio.")
        conn.close()
        return

    print("Processando arquivos de log e populando/atualizando o banco de dados...")
    inserted_count = process_log_files(full_log_content, conn)
    print(f"\n{inserted_count} novas mãos foram inseridas no banco de dados.")
    print("Banco de dados populado.")
    print("Para visualizar as estatísticas, execute o servidor web (app.py) e acesse no navegador.")

    conn.close()

if __name__ == "__main__":
    main()