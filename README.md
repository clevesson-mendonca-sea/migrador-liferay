# Liferay Content Migration Tool

Ferramenta para migração automatizada de conteúdo para o Liferay, incluindo páginas, pastas e web contents.

## Características

- Migração de páginas mantendo hierarquia
- Criação automática de estrutura de pastas
- Migração de conteúdo web com suporte a imagens e documentos
- Suporte a migração em lote via planilha Google Sheets
- Execução assíncrona para melhor performance
- Sistema de retry para itens que falharem
- Logging detalhado de operações e erros

## Pré-requisitos

- Python 3.8+
- Conta no Liferay com permissões administrativas
- Acesso à API do Google Sheets
- Planilha Google configurada com o mapeamento de conteúdo

## Dependências

```bash
pip install -r requirements.txt
```

Principais dependências:
- aiohttp
- beautifulsoup4
- python-dotenv
- gspread
- google-auth-oauthlib

## Configuração

### 1. Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
LIFERAY_URL=https://seu-liferay.com
LIFERAY_USERNAME=seu_usuario
LIFERAY_PASSWORD=sua_senha
LIFERAY_SITE_ID=seu_site_id
SPREADSHEET_ID=id_da_planilha_google
LIFERAY_CONTENT_STRUCTURE_ID=id_da_estrutura
FOLDER_TYPE=journal
```

### 2. Google Sheets API

#### Configurando o Acesso à Planilha

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um novo projeto ou selecione um existente
3. No menu lateral, vá em "APIs e Serviços" > "Biblioteca"
4. Busque e habilite as seguintes APIs:
   - Google Sheets API
   - Google Drive API
5. No menu lateral, vá em "APIs e Serviços" > "Credenciais"
6. Clique em "Criar Credenciais" > "ID do cliente OAuth"
7. Configure a tela de consentimento OAuth se necessário
8. Escolha "Aplicativo de Desktop" como tipo de aplicação
9. Baixe o arquivo de credenciais (JSON)
10. Renomeie o arquivo para `client_secret.json` e coloque na raiz do projeto

#### Obtendo o token.json

Na primeira execução do script:
1. Execute o script normalmente:
   ```bash
   python main.py
   ```
2. Uma janela do navegador será aberta automaticamente
3. Faça login com sua conta Google
4. Autorize o acesso à planilha
5. O arquivo `token.json` será gerado automaticamente na raiz do projeto

#### Notas Importantes

- O `client_secret.json` contém suas credenciais do Google Cloud e não deve ser compartilhado
- O `token.json` é gerado automaticamente e contém o token de acesso
- Se o token expirar, delete o arquivo `token.json` e execute o script novamente
- A conta Google usada para autorizar deve ter acesso à planilha
- O ID da planilha pode ser encontrado na URL do Google Sheets:
  ```
  https://docs.google.com/spreadsheets/d/[SPREADSHEET_ID]/edit
  ```

### 3. Tutorial Detalhado: Configurando Acesso ao Google Sheets

#### Parte 1: Criando um Projeto no Google Cloud

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Faça login com sua conta Google
3. No topo da página, clique no seletor de projetos
4. Clique em "Novo Projeto"
5. Preencha:
   - Nome do Projeto: ex: "Migração Liferay"
   - Localização: mantenha o padrão
6. Clique em "Criar"

#### Parte 2: Habilitando as APIs Necessárias

1. No menu lateral esquerdo, vá para "APIs e Serviços" > "Biblioteca"
2. Busque e habilite estas APIs (uma por vez):
   - Google Sheets API
   - Google Drive API
3. Para cada API:
   - Clique na API na lista de resultados
   - Clique no botão "Habilitar"
   - Aguarde a confirmação

#### Parte 3: Configurando a Tela de Consentimento

1. No menu lateral, vá para "APIs e Serviços" > "Tela de consentimento OAuth"
2. Selecione "Externo" e clique em "Criar"
3. Preencha as informações básicas:
   - Nome do app: "Migração Liferay"
   - Email de suporte: seu email
   - Email do desenvolvedor: seu email
4. Clique em "Salvar e Continuar"
5. Na seção "Escopos":
   - Clique em "Adicionar ou Remover Escopos"
   - Selecione:
     - `/auth/spreadsheets.readonly`
     - `/auth/drive.readonly`
   - Clique em "Atualizar"
6. Continue até o final do assistente

#### Parte 4: Criando Credenciais

1. No menu lateral, vá para "APIs e Serviços" > "Credenciais"
2. Clique em "Criar Credenciais" > "ID do Cliente OAuth"
3. Em "Tipo de Aplicativo" escolha "Aplicativo de Desktop"
4. Nome: "Migração Liferay Desktop"
5. Clique em "Criar"
6. Na janela pop-up, clique em "Fazer Download"
7. Renomeie o arquivo baixado para `client_secret.json`

#### Parte 5: Configurando o Projeto

1. Mova o arquivo `client_secret.json` para a pasta raiz do projeto
2. Na planilha Google que você vai usar:
   - Abra a planilha no navegador
   - Copie o ID da URL:
     ```
     https://docs.google.com/spreadsheets/d/[ESTE-É-O-ID]/edit
     ```
3. No arquivo `.env` do projeto, adicione:
   ```
   SPREADSHEET_ID=seu-id-da-planilha
   ```

#### Parte 6: Primeira Execução e Autorização

1. Execute o script:
   ```bash
   python main.py
   ```
2. Uma janela do navegador abrirá automaticamente
3. Selecione sua conta Google
4. Você verá um aviso "App não verificado"
   - Clique em "Avançado"
   - Clique em "Acessar [nome-do-projeto]"
5. Clique em "Permitir" para todas as permissões solicitadas
6. Volte para o terminal - o script continuará executando
7. O arquivo `token.json` será criado automaticamente

#### Parte 7: Verificando o Acesso

1. O script deve estar rodando sem erros de autenticação
2. Verifique se os dados da planilha estão sendo lidos corretamente
3. Se houver erros:
   - Verifique se o ID da planilha está correto
   - Confirme se sua conta tem acesso à planilha
   - Delete o `token.json` e tente novamente se necessário

#### Solução de Problemas do Google Sheets

1. Erro "client_secret.json não encontrado":
   - Verifique se o arquivo está na pasta correta
   - Confirme se o nome está exatamente como `client_secret.json`

2. Erro "Token inválido":
   ```bash
   rm token.json
   python main.py
   ```

3. Erro "Acesso negado à planilha":
   - Verifique se sua conta tem acesso à planilha
   - Compartilhe a planilha com sua conta
   - Tente novamente

4. Erro "API não habilitada":
   - Volte ao Google Cloud Console
   - Confirme se as APIs estão habilitadas
   - Aguarde alguns minutos - pode haver atraso na ativação

#### Dicas de Segurança para Credenciais

1. Nunca compartilhe ou comite os arquivos:
   - `client_secret.json`
   - `token.json`
2. Adicione ambos ao `.gitignore`
3. Mantenha as credenciais em local seguro
4. Revogue o acesso no Google Cloud Console se necessário

### 4. Planilha de Mapeamento

A planilha deve seguir o formato:
- Primeira linha: cabeçalho
- Segunda linha: em branco
- Colunas necessárias:
  - A: URL origem
  - B: URL destino
  - G: Hierarquia (separada por >)

## Uso

### Migração Completa

```bash
python main.py
```

### Migração Seletiva

Migrar apenas páginas:
```bash
python main.py --pages
```

Migrar apenas pastas:
```bash
python main.py --folders
```

Migrar apenas conteúdos:
```bash
python main.py --contents
```

## Logs e Monitoramento

- Logs gerais são exibidos no console
- Erros de migração de conteúdo são salvos em `content_migration_errors.txt`
- Use o nível de log INFO para acompanhar o progresso
- Erros detalhados são registrados em nível ERROR

## Estrutura do Projeto

```
.
├── main.py                 # Ponto de entrada
├── page_creator.py         # Criação de páginas
├── web_content_creator.py  # Migração de conteúdo
├── folder_creator.py   # Gerenciamento de pastas
├── url_utils.py           # Utilitários para URLs
├── document_creator.py    # Migração de documentos
├── .env                   # Configurações
├── client_secret.json     # Credenciais Google
└── requirements.txt       # Dependências
```

## Troubleshooting

### Erros Comuns

1. "No module named 'xxx'":
   - Verifique se todas as dependências estão instaladas
   - Use um ambiente virtual Python

2. "Invalid credentials":
   - Verifique as credenciais do Liferay no .env
   - Confirme se o token do Google não expirou
   - Verifique se o client_secret.json está correto

3. "Failed to fetch content":
   - Verifique se a URL de origem está acessível
   - Confirme se há conteúdo válido na página

4. Erros do Google Sheets:
   - "client_secret.json not found": Verifique se o arquivo está na raiz do projeto
   - "token.json is invalid": Delete o arquivo e execute novamente para reautenticar
   - "Insufficient permission": Verifique se a conta tem acesso à planilha
   - "Invalid SPREADSHEET_ID": Confirme se o ID da planilha no .env está correto

### Soluções

1. Para problemas com credenciais Google:
   ```bash
   rm token.json
   python main.py
   ```

2. Para reiniciar do zero:
   ```bash
   rm content_migration_errors.txt
   rm token.json
   python main.py
   ```

## Best Practices

1. Sempre faça backup antes de iniciar a migração
2. Execute testes com um subconjunto pequeno primeiro
3. Monitore os logs durante a execução
4. Verifique o arquivo de erros regularmente
5. Use um ambiente virtual Python

## Limitações

- Migração é feita de forma sequencial por página
- Algumas formatações complexas podem não ser preservadas
- Documentos muito grandes podem demorar mais para migrar

## Contribuindo

1. Fork o repositório
2. Crie uma branch para sua feature
3. Faça commit das mudanças
4. Push para a branch
5. Abra um Pull Request
