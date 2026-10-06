"""Suite de testes. Roda inteiramente em sqlite+aiosqlite em memoria.

A variavel `OTEL_SDK_DISABLED` e fixada aqui, antes de qualquer import de
`app.main`: importar o modulo executa `app = create_app()`, que configura a
telemetria (decisao D19). Sem isso a suite instalaria os providers do SDK e os
exportadores OTLP ficariam tentando alcancar um collector inexistente em
segundo plano. `setdefault` preserva uma escolha explicita do ambiente.
"""

import os

os.environ.setdefault("OTEL_SDK_DISABLED", "true")
