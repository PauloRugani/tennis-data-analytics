# Vulnerabilidades e Riscos Arquiteturais (complementar)

> Este arquivo reúne pontos que **não entraram** na versão enxuta de [auditoria_completa_pontos_de_melhoria.md](auditoria_completa_pontos_de_melhoria.md) (você removeu o que não fazia sentido priorizar agora), mais **descobertas novas** de uma verificação adicional em `dashboard/` e `notebooks/` que eu ainda não tinha auditado a fundo. Foco aqui é só em **vulnerabilidade de segurança** e **problema arquitetural** — não é lista de estilo/código.

---

## 🆕 Descobertas novas (não estavam em nenhum documento anterior)

### N1. 🔴 IP do servidor de produção do Postgres exposto em texto puro no Git
Os arquivos `.tmdl` do modelo semântico do Power BI, versionados no repositório, contêm o endereço público do banco de produção **hardcoded**:

```
Fonte = PostgreSQL.Database("129.213.23.114:5432", "postgres"),
```

Isso aparece repetido em pelo menos 8 arquivos:
- [dashboard/tennis-data-analytics-dashboard.SemanticModel/definition/tables/gold dim_date.tmdl](<dashboard/tennis-data-analytics-dashboard.SemanticModel/definition/tables/gold dim_date.tmdl>)
- `gold dim_entry.tmdl`, `gold dim_players.tmdl`, `gold dim_tournaments.tmdl`
- `gold fact_player_match_stats.tmdl`, `gold fact_player_ranking.tmdl`, `gold fact_player_season.tmdl`, `gold fact_player_tournament_stats.tmdl`

**Risco real:** qualquer pessoa com acesso de leitura ao repositório (inclusive se o repo for tornado público, um fork vazar, ou um colaborador externo) sabe exatamente **onde** está o banco de produção (IP + porta + nome do database). Isso reduz o trabalho de um atacante a apenas descobrir/forçar a credencial (usuário/senha), que felizmente **não** está no `.tmdl` (o Power BI guarda credencial separadamente, fora do arquivo versionado) — mas a superfície de ataque já está exposta publicamente no histórico do Git.
**Recomendação:** mover o banco para trás de uma VPN/allowlist de IP (não expor `5432` publicamente na internet), trocar a string de conexão para usar um hostname interno/gateway, e (se o repositório puder um dia ficar público) reescrever o histórico do Git para remover o IP dos commits antigos.