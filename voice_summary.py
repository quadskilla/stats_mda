import re
import speech_recognition as sr
import pyttsx3
from bs4 import BeautifulSoup


def load_stats_from_html(html_file):
    with open(html_file, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    players = {}
    for div in soup.find_all('div', class_='player-summary'):
        name_tag = div.find('h2')
        if not name_tag:
            continue
        name = name_tag.get_text(strip=True)
        stats = {}
        for line in div.find_all('div', class_='stat-line'):
            text = line.get_text(strip=True)
            if ':' in text:
                key, value = text.split(':', 1)
                stats[key.strip().lower()] = value.strip()
        players[name.lower()] = stats
    return players


def speak(text, engine):
    engine.say(text)
    engine.runAndWait()


SIZE_SYNONYMS = {
    'um quarto': '0-29%',
    'um terço': '30-45%',
    'meio pote': '46-56%',
    'baga': '57-70%',
    'pote': '71-100%',
    'over pote': '101%+'
}


def apply_size_shortcuts(text: str) -> str:
    """Replace verbal bet size shortcuts with their percentage range."""
    for phrase, replacement in SIZE_SYNONYMS.items():
        pattern = re.compile(phrase, re.IGNORECASE)
        text = pattern.sub(replacement, text)
    return text


def main():
    html_file = 'estatisticas_resumidas.html'
    stats = load_stats_from_html(html_file)

    recognizer = sr.Recognizer()
    tts_engine = pyttsx3.init()
    speak('Diga Jogador para iniciar. Diga sair para encerrar.', tts_engine)

    while True:
        with sr.Microphone() as source:
            audio = recognizer.listen(source)
        try:
            command = recognizer.recognize_google(audio, language='pt-BR')
            print(f"Comando reconhecido: {command}")
        except sr.UnknownValueError:
            continue
        except sr.RequestError:
            speak('Erro no serviço de reconhecimento de voz.', tts_engine)
            continue

        command = command.lower().strip()
        if command == 'sair':
            break
        if command != 'jogador':
            speak('Para iniciar, diga Jogador.', tts_engine)
            continue

        speak('Fale o jogador e a estatística.', tts_engine)
        with sr.Microphone() as source:
            audio = recognizer.listen(source)
        try:
            query = recognizer.recognize_google(audio, language='pt-BR')
            print(f"Consulta reconhecida: {query}")
        except sr.UnknownValueError:
            speak('Não entendi. Repita, por favor.', tts_engine)
            continue
        except sr.RequestError:
            speak('Erro no serviço de reconhecimento de voz.', tts_engine)
            continue

        query = query.lower().strip()
        if query == 'sair':
            break
        if ' e ' not in query:
            speak('Diga no formato: jogador e estatística.', tts_engine)
            continue

        nick, line = [p.strip() for p in query.split(' e ', 1)]
        line = apply_size_shortcuts(line)
        player_stats = stats.get(nick)
        if not player_stats:
            speak(f'Jogador {nick} não encontrado.', tts_engine)
            continue

        stat_value = None
        for key, value in player_stats.items():
            if line.lower() in key:
                stat_value = value
                break
        if not stat_value:
            speak('Estatística não encontrada.', tts_engine)
            continue

        result = 'neutra'
        if 'under' in stat_value.lower():
            result = 'under'
        elif 'over' in stat_value.lower():
            result = 'over'

        speak(f'{line} do jogador {nick}: {stat_value}. Resultado: {result}.', tts_engine)

    speak('Encerrando.', tts_engine)


if __name__ == '__main__':
    main()