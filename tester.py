import base64
import json
import os
import sys
from urllib import error, request

# PNG 1x1 (transparente) em base64
PNG_1X1_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="
# Se existir um arquivo "1.png" no diretório atual, carregue-o e substitua o PNG embutido
try:
    with open("1.png", "rb") as _f:
        PNG_1X1_B64 = base64.b64encode(_f.read()).decode("ascii")
except Exception:
    # Mantém o PNG padrão se falhar ao ler 1.png
    pass


def is_jpeg(data: bytes) -> bool:
    return len(data) > 4 and data[:3] == b"\xff\xd8\xff" and data[-2:] == b"\xff\xd9"


def is_mp3(data: bytes) -> bool:
    # Verifica tag ID3 ou frame sync (0xFF Ex) — heurística simples
    if len(data) < 3:
        return False
    if data[:3] == b"ID3":
        return True
    # frame sync: 0xFF followed by byte com 0b111xxxxx
    return data[0] == 0xFF and (data[1] & 0b11100000) == 0b11100000


def main() -> int:
    # Configurações
    api_url = os.getenv("API_URL", "http://localhost:5095/convert")
    token = os.getenv("AUTH_TOKEN", "")
    output_path = os.getenv("OUTPUT_PATH", "test_output.jpg")

    # Função utilitária para enviar conversão e validar resposta
    def run_test(data_b64: str, extension: str, expect_fn, out_path: str) -> bool:
        payload = {
            "data": data_b64,
            "async_mode": False,
            "extension": extension,
            "webhook_url": None,
            "webhook_headers": None,
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        data = json.dumps(payload).encode("utf-8")
        req = request.Request(api_url, data=data, headers=headers, method="GET")

        try:
            with request.urlopen(req, timeout=30) as resp:
                body = resp.read()
        except error.HTTPError as e:
            print(f"Falha HTTP {e.code}: {e.reason}", file=sys.stderr)
            try:
                print(e.read().decode("utf-8", "ignore"), file=sys.stderr)
            except Exception:
                pass
            return False
        except error.URLError as e:
            print(f"Erro de conexão: {e.reason}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"Erro inesperado: {e}", file=sys.stderr)
            return False

        if not body:
            print("Resposta vazia da API.", file=sys.stderr)
            return False

        if not expect_fn(body):
            print(
                f"Falha: resposta não parece ser do tipo esperado (ext={extension}).",
                file=sys.stderr,
            )
            try:
                with open(out_path.replace(".", "_unexpected."), "wb") as f:
                    f.write(body)
                print(
                    f"Conteúdo salvo em {out_path.replace('.', '_unexpected.')} para análise."
                )
            except Exception:
                pass
            return False

        try:
            with open(out_path, "wb") as f:
                f.write(body)
            print(f"Sucesso: conversão salva em: {out_path}")
        except Exception as e:
            print(f"Falha ao salvar saída: {e}", file=sys.stderr)
            return False

        return True

    # 1) Teste de imagem (PNG -> JPEG)
    if len(sys.argv) > 1:
        in_path = sys.argv[1]
        try:
            with open(in_path, "rb") as f:
                png_bytes = f.read()
            data_b64 = base64.b64encode(png_bytes).decode("ascii")
        except Exception as e:
            print(f"Falha ao ler arquivo de entrada: {e}", file=sys.stderr)
            return 1
    else:
        data_b64 = PNG_1X1_B64

    ok_img = run_test(data_b64, "png", is_jpeg, output_path)

    # 2) Teste de áudio: usa telegram_audio.ogg no repositório -> espera MP3
    audio_in_path = "telegram_audio.ogg"
    audio_out = os.getenv("OUTPUT_AUDIO_PATH", "test_output_audio.mp3")
    try:
        with open(audio_in_path, "rb") as f:
            audio_bytes = f.read()
        audio_b64 = base64.b64encode(audio_bytes).decode("ascii")
        ok_audio = run_test(audio_b64, "ogg", is_mp3, audio_out)
    except FileNotFoundError:
        print(
            f"Arquivo de áudio de teste não encontrado: {audio_in_path}",
            file=sys.stderr,
        )
        ok_audio = False
    except Exception as e:
        print(f"Falha ao ler arquivo de áudio: {e}", file=sys.stderr)
        ok_audio = False

    # Código de saída: 0 se ambos OK, 2 se apenas um, 1 se nenhum
    if ok_img and ok_audio:
        return 0
    if ok_img or ok_audio:
        return 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
