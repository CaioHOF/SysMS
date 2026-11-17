# Sistema de mensagens

Projeto de troca de mensagens em canais públicos para a disciplina de Sistemas Distribuídos. Foi feito individualmente, usando Java no bot e Python nos servidores e serviços auxiliares.

## Como executar

Com o Docker aberto, execute na pasta do projeto:

```text
docker compose up --build
```

O Compose inicia o broker, o proxy, o serviço `ref`, dois servidores e dois bots. Para encerrar, pressione `Ctrl+C` e execute `docker compose down`.

## Como funciona

- Os bots fazem login e consultam os canais disponíveis. Se houver menos de cinco, criam outros. Depois escolhem e assinam três canais e continuam enviando mensagens aleatórias: dez por rodada, com um segundo entre elas.
- Req/Rep é usado quando o bot precisa de uma resposta direta, por exemplo ao fazer login ou publicar. O bot envia o pedido ao broker, que o encaminha para um servidor; esse servidor devolve a resposta pelo mesmo caminho.
- Pub/Sub é usado para divulgar uma publicação a todos os bots inscritos naquele canal. O servidor publica a mensagem com o nome do canal como tópico, e o proxy a encaminha aos bots inscritos.
- As mensagens são serializadas em binário com MessagePack e incluem um timestamp e um contador lógico. Ao receber uma mensagem, o processo atualiza seu contador para o maior valor entre o contador local e o recebido. Os logs do cliente e do servidor mostram o conteúdo das mensagens.

Quando um bot publica, o servidor verifica se o canal existe e se o texto não está vazio. Se estiver tudo certo, registra e divulga a publicação no canal, além de devolver ao bot o status da operação. Uma publicação recebida por um bot mostra quem enviou, em qual canal e os horários envolvidos.

## Servidores, relógios e dados

O serviço `ref` atribui um rank a cada servidor e mantém a lista dos que estão ativos. Os servidores enviam heartbeat a cada dez pedidos de clientes; após 30 segundos sem heartbeat, o servidor deixa de aparecer como ativo.

O servidor ativo de maior rank é o coordenador. A cada 15 pedidos de clientes, os outros servidores sincronizam seus relógios com ele pelo algoritmo de Berkeley. Se o coordenador não responder, os servidores consultam os outros e fazem uma eleição. O resultado é divulgado pelo tópico `servers` do Pub/Sub.

Cada servidor grava em um volume Docker próprio um arquivo JSON com usuários, horários de login, canais, pedidos e publicações. Como o broker distribui os pedidos entre os servidores, cada um pode receber apenas parte deles. Para manter cópias dos dados, cada servidor publica eventos de replicação; um identificador evita registrar o mesmo evento mais de uma vez. Ao iniciar, um servidor pede uma cópia completa aos servidores ativos. Também há novos pedidos de cópia a cada 15 pedidos de clientes. A replicação é feita pelo Pub/Sub e pode levar um pequeno intervalo para chegar aos outros servidores.

O servidor oferece a operação `list_messages` para recuperar publicações anteriores. O pedido precisa incluir `username`, `timestamp` e `logical_clock`; pode incluir `channel` para filtrar por canal e `offset` e `limit` para consultar uma página de até 100 mensagens. A resposta informa o total e se há mais páginas. O bot atual não consulta o histórico automaticamente.

O Compose desta entrega inicia dois servidores. Para adicionar outro, é preciso incluir um serviço com nome e volume próprios. O servidor novo pede uma cópia dos dados antes de receber pedidos; se um servidor parar, os outros continuam ativos.

## Pastas principais

- `src/cliente`: bot em Java.
- `src/servidor`: servidores em Python.
- `src/broker`: broker Req/Rep.
- `src/proxy`: proxy Pub/Sub.
- `src/ref`: registro e ranks dos servidores.
- `docker-compose.yaml`: configuração dos serviços e volumes.
