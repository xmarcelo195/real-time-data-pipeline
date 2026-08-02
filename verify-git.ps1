# Salve como verify-git.ps1 e execute na raiz do repositório
# Exemplo de execução: .\verify-git.ps1

# Função para buscar padrões suspeitos
function Scan-Files {
    param(
        [string]$Path = "."
    )

    Write-Host "Iniciando verificação em: $Path" -ForegroundColor Cyan

    # Extensões para analisar
    $exts = @(
    "*.py",       # código Python
    "*.ipynb",    # notebooks Jupyter
    "*.txt",      # arquivos de texto
    "*.md",       # README / documentação
    "*.toml",     # configuração (ex: pyproject.toml)
    "*.cfg",      # configuração
    "*.json",     # arquivos JSON
    "*.yml",      # YAML
    "*.yaml",     # YAML
    "*.sh"        # scripts de shell
)

    foreach ($ext in $exts) {
        Get-ChildItem -Path $Path -Recurse -Include $ext -File | ForEach-Object {
            $file = $_.FullName
            $content = Get-Content $file -Raw

            # Padrões suspeitos
            $patterns = @(
                "eval\(",           # execução dinâmica de código
                "Function\(",       # execução dinâmica de código
                "fetch\(",          # requisições externas
                "XMLHttpRequest",   # requisições externas
                "child_process",    # NodeJS exec
                "wasm",             # possíveis cargas WASM
                "base64_decode",    # decodificação de payloads
                "unsafe"            # Rust unsafe blocks
            )

            foreach ($p in $patterns) {
                if ($content -match $p) {
                    Write-Host "⚠ Padrão suspeito encontrado em $file : $p" -ForegroundColor Yellow
                }
            }
        }
    }

    Write-Host "Verificação concluída." -ForegroundColor Green
}

# Executa a função
Scan-Files -Path "."
