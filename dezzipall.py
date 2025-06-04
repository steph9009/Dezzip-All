import os
import subprocess
import logging
import argparse
from pathlib import Path
import sys
import platform # Per determinare il percorso predefinito di 7z

# --- Configurazione del Logging ---
def setup_logging(log_file='extraction.log'):
    """Configura il sistema di logging per file e console."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Logger principale
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # Livello base, cattura INFO, WARNING, ERROR, CRITICAL

    # Handler per scrivere su file
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    # Handler per scrivere sulla console
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    logging.info("Logging configurato. Log salvati in: %s", log_file)

# --- Funzione Principale di Estrazione ---
def recursive_extract(work_dir, path_to_7z, max_iterations=5):
    """
    Estrae ricorsivamente archivi trovati in work_dir usando 7z.
    Sovrascrive il contenuto se la cartella di destinazione esiste.
    Non elimina gli archivi originali.
    """
    work_path = Path(work_dir).resolve() # Ottiene il percorso assoluto e risolto
    path_to_7z = Path(path_to_7z).resolve()
    
    if not work_path.is_dir():
        logging.error("La directory di lavoro specificata non esiste: %s", work_path)
        return
    if not path_to_7z.is_file():
        logging.error("L'eseguibile 7z non è stato trovato in: %s", path_to_7z)
        return

    logging.info("Inizio estrazione ricorsiva in: %s", work_path)
    logging.info("Utilizzo 7-Zip da: %s", path_to_7z)
    logging.info("Numero massimo di iterazioni: %d", max_iterations)
    logging.warning("Gli archivi originali NON verranno eliminati.")
    logging.warning("Il contenuto delle cartelle esistenti verrà sovrascritto durante l'estrazione.")

    processed_archives = set() # Tiene traccia degli archivi già processati in questa esecuzione
    archive_extensions = ['.zip', '.7z', '.tar', '.gz', '.tgz'] # Aggiungi altre estensioni se necessario (es. .rar, .bz2)
    total_processed_successfully = 0
    total_errors = 0

    for iteration in range(max_iterations):
        logging.info("--- Inizio Iterazione %d di %d ---", iteration + 1, max_iterations)
        archives_found_this_iteration = []
        
        # Trova tutti i file con le estensioni specificate ricorsivamente
        for ext in archive_extensions:
            archives_found_this_iteration.extend(list(work_path.rglob(f'*{ext}')))
            # Gestione specifica per .tar.gz, .tar.bz2 etc. (rglob non li prende con *.gz se sono *.tar.gz)
            if ext in ['.gz', '.bz2', '.xz']: 
                 archives_found_this_iteration.extend(list(work_path.rglob(f'*.tar{ext}')))

        # Rimuovi duplicati nel caso di find con .tar.gz e .gz
        unique_archives_found = list(set(archives_found_this_iteration))

        if not unique_archives_found:
            logging.info("Nessun file archivio trovato in questa iterazione.")
            break # Interrompe il ciclo se non ci sono archivi

        newly_processed_in_iteration = 0
        for archive_path in unique_archives_found:
            archive_full_path_str = str(archive_path)

            # Salta se già processato
            if archive_full_path_str in processed_archives:
                continue

            logging.info("Trovato archivio da processare: %s", archive_full_path_str)

            archive_name = archive_path.name
            archive_dir = archive_path.parent

            # Determina il nome della cartella di destinazione (nome base senza estensione)
            # Gestisce estensioni doppie come .tar.gz
            stem = archive_path.stem 
            if archive_path.suffix.lower() in ['.gz', '.bz2', '.xz'] and stem.lower().endswith('.tar'):
                 base_name = Path(stem).stem # Rimuove anche .tar
            else:
                 base_name = stem
                 
            extract_dir = archive_dir / base_name

            logging.info("Tentativo di estrazione in: %s", extract_dir)

            # Crea la directory di destinazione se non esiste. Non fallisce se esiste.
            try:
                os.makedirs(extract_dir, exist_ok=True)
                logging.debug("Directory di estrazione assicurata: %s", extract_dir)
            except OSError as e:
                logging.error("Errore nella creazione della directory di estrazione %s per l'archivio %s. Errore: %s", 
                              extract_dir, archive_full_path_str, e)
                total_errors += 1
                processed_archives.add(archive_full_path_str) # Segna come 'processato' per non riprovare all'infinito
                continue # Passa al prossimo archivio

            # Costruisce ed esegue il comando 7z
            # 'x' = estrai con percorsi completi
            # '-o' = directory di output (senza spazi)
            # '-y' = sì a tutte le domande (sovrascrittura)
            command = [str(path_to_7z), 'x', archive_full_path_str, f'-o{extract_dir}', '-y']
            
            try:
                logging.debug("Esecuzione comando: %s", " ".join(command))
                result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='replace')

                # Controlla il risultato dell'esecuzione
                if result.returncode == 0:
                    logging.info("Estrazione di %s completata con successo.", archive_name)
                    total_processed_successfully += 1
                    newly_processed_in_iteration += 1
                    processed_archives.add(archive_full_path_str)
                elif result.returncode == 1:
                    # Codice 1: Warning (spesso non fatale, es. file bloccati non sovrascritti)
                    logging.warning("Estrazione di %s completata con Warning (Codice %d). Output:\nSTDOUT:\n%s\nSTDERR:\n%s",
                                    archive_name, result.returncode, result.stdout.strip(), result.stderr.strip())
                    total_processed_successfully += 1 # Consideralo successo parziale/completo
                    newly_processed_in_iteration += 1
                    processed_archives.add(archive_full_path_str) 
                else:
                    # Codice 2 (Errore Fatale) o altri errori
                    logging.error("Errore durante l'estrazione di %s (Codice %d). Comando: %s\nSTDOUT:\n%s\nSTDERR:\n%s",
                                  archive_name, result.returncode, " ".join(command), result.stdout.strip(), result.stderr.strip())
                    total_errors += 1
                    # Non aggiungere a processed_archives se l'errore è fatale (codice 2), 
                    # potrebbe essere ritentato (anche se improbabile che funzioni senza intervento). 
                    # Aggiungilo per altri errori per evitare loop infiniti.
                    if result.returncode != 2:
                         processed_archives.add(archive_full_path_str)

            except FileNotFoundError:
                logging.error("Errore: Impossibile trovare l'eseguibile 7z in %s. Assicurati che il percorso sia corretto.", path_to_7z)
                return # Interrompe lo script se 7z non è trovatos
            except Exception as e:
                logging.exception("Errore imprevisto durante l'esecuzione di 7z per %s: %s", archive_full_path_str, e)
                total_errors += 1
                processed_archives.add(archive_full_path_str) # Segna come processato per evitare loop

        if newly_processed_in_iteration == 0 and iteration > 0:
             logging.info("Nessun *nuovo* archivio processato in questa iterazione. Probabile fine del lavoro utile.")
             # Potremmo uscire qui, ma continuiamo fino a max_iterations per sicurezza / semplicità
             # break 

    logging.info("--- Processo di Estrazione Terminato ---")
    logging.info("Iterazioni completate: %d", iteration + 1)
    logging.info("Archivi processati con successo (o warning): %d", total_processed_successfully)
    logging.info("Errori riscontrati durante l'estrazione: %d", total_errors)
    logging.info("Numero totale di percorsi di archivio unici trovati e considerati: %d", len(processed_archives) + total_errors) # Stima approssimativa

# --- Blocco Principale di Esecuzione ---
if __name__ == "__main__":
    # Determina percorso predefinito per 7z basato sul SO
    default_7z_path = ""
    if platform.system() == "Windows":
        # Prova percorsi comuni su Windows
        program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
        program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
        if Path(program_files, "7-Zip", "7z.exe").is_file():
            default_7z_path = str(Path(program_files, "7-Zip", "7z.exe"))
        elif Path(program_files_x86, "7-Zip", "7z.exe").is_file():
             default_7z_path = str(Path(program_files_x86, "7-Zip", "7z.exe"))
        else:
             default_7z_path = "7z.exe" # Prova a vedere se è nel PATH
    elif platform.system() == "Linux" or platform.system() == "Darwin":
        # Su Linux/Mac, 7z è spesso nel PATH
        default_7z_path = "7z" 
    else:
        default_7z_path = "7z" # Default generico

    parser = argparse.ArgumentParser(description='Estrae ricorsivamente archivi usando 7z, sovrascrivendo il contenuto esistente.')
    parser.add_argument('work_dir', type=str, help='Directory radice contenente gli archivi da estrarre.')
    parser.add_argument('--path_to_7z', type=str, default=default_7z_path,
                        help=f'Percorso completo dell\'eseguibile 7z.exe (o 7z). Default: "{default_7z_path}"')
    parser.add_argument('--max_iterations', type=int, default=5,
                        help='Numero massimo di iterazioni per gestire archivi annidati. Default: 5')
    parser.add_argument('--log_file', type=str, default='extraction.log',
                        help='Nome del file di log. Default: extraction.log')

    args = parser.parse_args()

    setup_logging(args.log_file)

    try:
        recursive_extract(args.work_dir, args.path_to_7z, args.max_iterations)
    except Exception as e:
        logging.exception("Errore critico non gestito durante l'esecuzione dello script: %s", e)
        sys.exit(1) # Esce con codice di errore

    logging.info("Script completato.")
    sys.exit(0)
