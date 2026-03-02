# Setup da Integração Supabase

## Problema Atual
As tabelas do Supabase não aparecem porque:
1. A função RPC necessária não existe no seu Supabase
2. Ou não há tabelas criadas no database

## Solução

### Passo 1: Acessar o Supabase SQL Editor
1. Acesse https://supabase.com/dashboard
2. Selecione seu projeto
3. Vá em **SQL Editor** no menu lateral
4. Clique em **New Query**

### Passo 2: Executar o Script de Setup
Copie e cole o conteúdo do arquivo `supabase_setup.sql` e execute.

Este script cria:
- Função `get_tables_in_schema()` que lista todas as tabelas de um schema
- Permissões para que a API possa chamar esta função

### Passo 3: Verificar se Você Tem Tabelas

Execute no SQL Editor:

```sql
-- Ver tabelas no schema public
SELECT tablename 
FROM pg_tables 
WHERE schemaname = 'public';

-- Ver todos os schemas que você tem
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name;
```

### Passo 4: (Opcional) Criar Tabelas de Teste

Se você ainda não tem tabelas, crie algumas para testar:

```sql
-- Criar tabela de exemplo
CREATE TABLE IF NOT EXISTS public.clientes (
  id SERIAL PRIMARY KEY,
  nome TEXT NOT NULL,
  email TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.pedidos (
  id SERIAL PRIMARY KEY,
  cliente_id INTEGER REFERENCES clientes(id),
  valor DECIMAL(10,2),
  status TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Inserir dados de teste
INSERT INTO public.clientes (nome, email) VALUES 
  ('João Silva', 'joao@example.com'),
  ('Maria Santos', 'maria@example.com');

INSERT INTO public.pedidos (cliente_id, valor, status) VALUES 
  (1, 150.00, 'aprovado'),
  (1, 200.00, 'pendente'),
  (2, 350.00, 'aprovado');
```

### Passo 5: Testar a Conexão

Após executar o script, volte para a aplicação e:
1. Acesse a página de **Criar Dataset**
2. Selecione o projeto **SISTEMA ORÇAMENTOS**
3. Selecione a área **Orçamentos (220)**
4. Escolha **SUPABASE** como tipo de fonte
5. As tabelas devem aparecer agora!

## Verificação Rápida

Teste se a função foi criada corretamente:

```sql
SELECT * FROM get_tables_in_schema('public');
```

Se retornar suas tabelas, está funcionando! 🎉

## Troubleshooting

### "Function does not exist"
Execute o script `supabase_setup.sql` novamente.

### "Tables still not showing"
1. Verifique se as tabelas realmente existem: `SELECT * FROM pg_tables WHERE schemaname = 'public';`
2. Verifique se o `.env` tem as credenciais corretas:
   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_PUBLISHABLE_KEY`
   - `SUPABASE_ACCESS_TOKEN`

### "Permission denied"
A função precisa de `SECURITY DEFINER` para funcionar. Verifique se executou todo o script.

## Schemas Personalizados

Se você usa um schema diferente de `public` (como `orcamentos_cm`), você precisa:

1. Garantir que as tabelas estão nesse schema
2. Ajustar as queries para usar o schema correto

```sql
-- Criar tabela em schema personalizado
CREATE SCHEMA IF NOT EXISTS orcamentos_cm;

CREATE TABLE IF NOT EXISTS orcamentos_cm.orcamentos (
  id SERIAL PRIMARY KEY,
  descricao TEXT,
  valor DECIMAL(10,2),
  data_criacao TIMESTAMP DEFAULT NOW()
);
```

## Suporte

Se após seguir todos os passos ainda não funcionar:
1. Verifique os logs do backend (janela PowerShell do backend)
2. Verifique o console do navegador (F12)
3. Teste a API diretamente: http://localhost:3001/api/portal/supabase/tables?schema=public
