# Sistema de mensagens - Parte 2

Nesta parte, os bots publicam mensagens em canais e recebem as mensagens dos canais em que se inscreveram.

## Como executar

Na pasta do projeto, execute:

```bash
docker compose up --build
```

O Compose inicia o proxy, dois servidores e dois bots. Os bots criam canais até haver pelo menos cinco, escolhem três canais para acompanhar e publicam mensagens continuamente. Para encerrar, use `Ctrl+C`.

## Troca de mensagens

As requisições de login, listagem, criação de canal e publicação usam o padrão REQ/REP da Parte 1. O proxy Pub/Sub usa XSUB na porta 5557 para receber publicações dos servidores e XPUB na porta 5558 para encaminhá-las aos clientes inscritos.

As mensagens da aplicação são serializadas em MessagePack e incluem timestamp em milissegundos. O servidor persiste em seu próprio arquivo os usuários, canais, requisições e respostas, além das publicações. Os volumes dos dois servidores são separados.

Cada bot escolhe três canais aleatórios da lista disponível. Depois, em loop, escolhe um canal e envia dez mensagens aleatórias, com um segundo entre envios. O terminal mostra as mensagens publicadas e as recebidas, incluindo canal e timestamps de envio e recebimento.
