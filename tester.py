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


def main() -> int:
    # Configurações
    api_url = os.getenv("API_URL", "http://localhost:5000/convert")
    token = os.getenv("AUTH_TOKEN", "1234")
    output_path = os.getenv("OUTPUT_PATH", "test_output.jpg")

    # Permite informar um arquivo PNG via argumento: python tester.py caminho/para/arquivo.png
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

    payload = {
        "data": data_b64,  # servidor aceita base64 em string
        "async_mode": False,
        "extension": "png",
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
        return 1
    except error.URLError as e:
        print(f"Erro de conexão: {e.reason}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Erro inesperado: {e}", file=sys.stderr)
        return 1

    if not body:
        print("Resposta vazia da API.", file=sys.stderr)
        return 1

    if not is_jpeg(body):
        print(
            "Falha: resposta não parece ser JPEG (assinatura inválida).",
            file=sys.stderr,
        )
        # Salva para diagnóstico
        try:
            with open("unexpected_response.bin", "wb") as f:
                f.write(body)
            print("Conteúdo salvo em unexpected_response.bin para análise.")
        except Exception:
            pass
        return 1

    # Salva o JPG convertido para inspeção manual
    try:
        with open(output_path, "wb") as f:
            f.write(body)
        print(f"Sucesso: PNG convertido para JPEG. Arquivo salvo em: {output_path}")
    except Exception as e:
        print(f"Conversão ok, mas falha ao salvar saída: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
