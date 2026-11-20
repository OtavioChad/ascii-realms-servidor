# Servidor de salas do ASCII Realms

Relay WebSocket sem dependências (Node.js 18+). Quem cria a sala recebe um código de 5 letras;
os amigos entram com o código. O servidor só repassa os pacotes entre os jogadores da mesma sala
e não guarda nada (nem saves, nem contas).

- Rodar: `node relay-server.js` (porta 8080 ou a variável `PORT`)
- Saúde: `GET /health`
- No jogo: `wss://<seu-endereço>` no campo "Servidor de salas" (ou no arquivo `servidor.txt`).
