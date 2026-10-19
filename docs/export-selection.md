# DEV09 — CP-DESIGN: exact-child composite export v1

Дата: 2026-10-19. Автор: D / gpt-6.1-sol / ultra. Статус: CP-DESIGN принят независимым reviewer; implementation и запуск fixtures ещё не выполнены.

## Задача и исходники

Обеспечить, чтобы один composite export использовал один exact CalcJob child для inventory, frozen plan, raw bytes и scientific status. Разрешение omitted selectors сохраняет существующую authority: published selection либо единственный legacy child. Новые scientific selection rules, эвристика latest, mutation selection и native daemon не входят.

Immutable source packet:

- Root `/home/tolya/.codex/worktrees/review-closure-exec/qcl-negf`, HEAD `e0f244d075dd49874ba4f3847f4efa693f3b3622`; `git status --short` при начале: только чужой untracked `docs/implementation/review-closure-2026-10-19/execution/`.
- AiiDA `53e2a20544a469dd7ee5560243ebe530a6310f0a`, исходно чистый: `src/aiida_qcl_negf/service.py`, `data.py`, `parser.py`, `tests/test_artifact_locator.py`, `tests/test_export_plan.py`, `docs/service.md`.
- Portal `36837e98a8d1f85820f2807d0212ec92e73c3c7b`: `src/qcl_negf_api/exports.py`, `api.py`, `service.py`, `tests/test_exports.py`, `tests/test_api.py`.
- Contracts `e5c056c47fc3bde3e0ca32866692d740ef9b325c`: `src/qcl_negf_contracts/messages.py::TERMINAL` и `schemas/scientific-worker-result.schema.json#/properties/status`.
- Results `fe27a2a9ed065f9afacecc84113311f30d0c68d6`: `src/qcl_negf_results/export.py::export_snapshot`, где `job_status` включён в snapshot identity, а `job_complete` определяется membership в `TERMINAL`.
- Scope: root `docs/architecture.md`; `docs/implementation/review-closure-2026-10-19/{autonomy,operator-permissions,development}.md`, DEV09–11. Activation EXEC и модель переданы координатором; прежнее PLAN в handoff не подменяется новым неявным grant.
- Historical evidence: `docs/implementation/comprehensive-review/2026-10-14-e48e5b2-01/{PORTABILITY.md,RV07.md,fixes/RV07-attempt-export.md}`. Исторические AiiDA/Portal refs отличаются от current source; finding перепроверен в current source, исторические файлы не исполнялись.

Полный `git submodule status --recursive` прочитан; состав определяется существующими gitlinks, новый maintained lock не создаётся. Этап этой карточки — source design; scientific arrays, HDF5, environment/import/test/build/server/solver не использованы.

## Выбранное решение и альтернативы

Выбрано resolve-once через две additive service operations. Portal один раз получает scalar binding, затем передаёт **оба** его selectors во все обращения к выбранному child. При смене published selection уже созданный snapshot не переключается.

Повторный default resolve в каждом вызове отвергнут: изменение selection между inventory и streaming допускает разные origins. Фильтрация all-attempt inventory в Portal отвергнута: Portal не владеет selection authority и не должен повторять её правила. Дополнительный opaque server-side selection token не нужен: immutable AiiDA child UUID вместе с проверяемым attempt уже задаёт exact identity и сохраняет existing service model.

## Additive Python API

```python
get_export_selection(identifier: str, execution_id: str, *,
                     attempt=None, calcjob_uuid=None) -> dict[str, Any]

list_execution_artifacts(identifier: str, execution_id: str, *,
                         attempt=None, calcjob_uuid=None) -> list[dict[str, Any]]
```

Обе операции synchronous, выполняются в existing loaded-profile AiiDA actor. ORM nodes, repositories и file handles наружу не возвращаются. Они не submit/cancel/retry, не пишут provenance и не меняют selection.

`identifier` — full process UUID existing `_node`, тот же QCL plan WorkChain boundary. UUID с полным 128-bit значением нормализуется `str(UUID(value))`; сокращённый prefix и нестроковое значение отвергаются. `execution_id` — строка длиной 1–128, без coercion; lookup остаётся точным, без case/whitespace normalization. `attempt` — `None` либо `type(value) is int and value >= 1`: bool, float, numeric string, zero и negative invalid. `calcjob_uuid` — `None` либо строка, успешно разбираемая `UUID`, нормализованная в lowercase dashed UUID. `None` соответствует отсутствующему selector; пустая строка не соответствует отсутствию.

Результат `get_export_selection` имеет **ровно четыре** поля:

| Поле | Тип и значение |
| --- | --- |
| `execution_id` | Запрошенная строка, равная selected child input execution ID |
| `attempt` | Положительный Python int selected child; отсутствие legacy `inputs.attempt` означает 1, как existing `_attempt`; присутствующее invalid значение не исправляется |
| `calcjob_uuid` | Canonical `str(UUID(child.uuid))`, lowercase dashed; invalid stored identity отвергается |
| `status` | Один из `running`, `completed`, `completed_with_warnings`, `failed`, `cancelled`, `paused`, `unknown` по правилам ниже |

Не добавляются `exit_status`, `is_finished_ok`, ORM object, selected result body, inferred convergence или `scientific_accepted`.

## Разрешение одного child

Обе новые операции используют существующий `_select_child` как selector, сохраняя его authority. New-operation validation/translation выполняется вокруг него; глобальные semantics existing `_select_child` и existing single-file calls не изменяются.

1. Проверить scalar input types и нормализовать UUID selectors до поиска child и чтения result/inventory.
2. Загрузить `_node(identifier)`, рассматривать только CalcJob descendants этого run с exact `execution_id`. Не выполнять глобальный lookup чужого UUID ради уточнения ошибки.
3. При хотя бы одном explicit selector published selection не читается и не применяется. Attempt-only должен оставить ровно одного child; UUID-only точно идентифицирует child; оба selectors обязаны указывать один child.
4. Для explicit UUID, принадлежащего этому run/execution, несовпадение переданного attempt с его stored attempt — `ValueError`/422. UUID вне этого run/execution либо отсутствующий — `LookupError`/404, даже если переданный attempt существует. Это не раскрывает чужой run. Отсутствующий attempt при attempt-only — 404. Несколько соответствующих child — 422. Никакого fallback после failed explicit lookup нет.
5. Если оба selectors omitted, использовать existing published selection для exact execution. Несколько published records — conflict/422. Единственный record обязан быть object с корректными `execution_id`, positive strict-int `attempt`, full canonicalizable `calcjob_uuid` и идентифицировать реальный child того же execution. Malformed, foreign или mismatched published record — invalid metadata/422, без fallback на single child. Дополнительные parser поля published record допустимы и не меняют выбор.
6. Без published record один child разрешён, включая legacy missing attempt=1; больше одного — ambiguity/422; ни одного — 404. Сортировка `ctime` в `_children` не выбирает latest.
7. New-operation wrapper фиксирует effective attempt/UUID при единственном чтении applicable published record либо legacy child. Он передаёт **оба effective selectors** в `_select_child`; повторный вызов `_select_child` с omitted selectors после проверки record запрещён, чтобы default не перечитался при гонке публикации. Explicit inputs также передаются как exact pair после описанных проверок. После выбора проверить stored child UUID/attempt. В `get_export_selection` один раз прочитать только result metadata и зафиксировать status. В `list_execution_artifacts` читать только inventory metadata; status для этой операции не перечитывается.

До успешного selector validation и child binding запрещены repository open/walk/seek/stat/hash и численные payload reads. Чтение scalar ORM attributes допускается. `get_export_selection` после binding может прочитать один file-backed **result metadata** node по ограничению ниже; это отличимо от чтения numerical artifacts. `list_execution_artifacts` не выполняет никакого repository I/O.

## Scientific status: источник, unknown и freeze

Источник известного статуса — только `selected_child.outputs.result`, опубликованный existing `QCLExecutionParser.parse` как immutable file-backed `series_result.json`. Используется existing `read_json(..., limit=MAX_RESULT_BYTES)` с максимумом 32 MiB и probe одним дополнительным byte; handle закрывается на всех путях. Whole-run `get_run`, another child, WorkChain state, scheduler state, `exit_status` и `is_finished_ok` не используются.

| Случай selected child | Результат |
| --- | --- |
| `outputs.result` отсутствует | `status="unknown"`, независимо от process state/exit0/exit failure |
| Node присутствует; bounded JSON object содержит recognized строковый `status` и exact `selected_execution_id == execution_id` | Вернуть status буквально, включая `running` и `paused` |
| Node присутствует, но open/read невозможен, metadata oversized, invalid UTF-8/JSON/nonfinite/duplicate key, не object, отсутствует/invalid `status`, отсутствует/foreign `selected_execution_id` | Отказ; это не missing result и не `unknown` |

Для present metadata допустимы ровно шесть source-status значений: `running`, `completed`, `completed_with_warnings`, `failed`, `cancelled`, `paused`. `unknown` возвращается только при отсутствующем `outputs.result`; строка `unknown` в присутствующем result node является invalid metadata. Проверяются bounded decoding, тип/enum status и execution binding. Полная научная проверка schema/coordinates/commit hashes/point convergence уже принадлежит parser и Results; новая операция её не дублирует и не заявляет повторную приёмку всех остальных result fields. Поэтому copied `completed` остаётся recorded scientific series status, а не независимым доказательством convergence, adequacy или экспериментальной валидации. Поле `unknown` не добавляется в scientific result schema и не записывается в исходные результаты: это только export-observation status.

`status` фиксируется в первом `get_export_selection` перед cache lookup и preparation. Portal сохраняет его неизменяемым в export entry, передаёт буквально как Results `job_status` и возвращает в GET status. Никакой normalization `paused/unknown -> running` нет. Existing Results не включает `unknown`, `paused` или `running` в `TERMINAL`, поэтому они не становятся `job_complete=true`; Results по-прежнему владеет completeness и scientific acceptance. Export `state=ready` означает готовность файла; оно независимо от scientific `status`.

При поздней публикации result node другой запрос может получить иной status для того же child. Это новый наблюдаемый snapshot; уже preparing/ready entry и выданный `export_id` свой status не меняют.

## Exact inventory

`list_execution_artifacts` самостоятельно разрешает child тем же new-operation selector contract. После binding требует selected `retrieved` и `inventory`; отсутствие одного — 404. Не обращается к unselected inventories и не использует `list_artifacts`.

Cached inventory должен быть object с `complete is True` и `files` типа list длиной не более `MAX_ARTIFACTS=10000`. `complete=False`, missing/неboolean marker, malformed files/row, повторный path или превышение limit — 422. Никакой truncation или repair. Каждая row содержит строковый непустой normalized relative POSIX `path` по existing `_artifact_path` и `size` типа strict int >=0; bool/float/negative size invalid. Extra source metadata не переносится в response.

Возврат — новый list, отсортированный по `path`, с каждой row **ровно** из `execution_id`, `attempt`, `calcjob_uuid`, `path`, `size`. Все identity fields построены из selected child, а не перекрываются source row. Empty complete inventory возвращает `[]`; Portal отказывает в создании empty-source export с 404. Дубликаты path у unselected child не имеют значения; дубликаты внутри selected inventory invalid. Это logical metadata bound, не измерение RAM/DB latency.

## Portal consumer и cache

Compatible signature:

```python
ExportStore.create(run, execution_id, profile, *, attempt=None, calcjob_uuid=None)
```

Existing HTTP route `POST /api/v1/runs/{run_uuid}/exports` сохраняется. Body сохраняет `execution_id` и `profile` (`science` по умолчанию, либо `full-state`) и additive optional `attempt`, `calcjob_uuid`. Wire values: attempt — JSON integer >=1, UUID — JSON string с full 128-bit UUID; omitted/null оба означают None. Bool/float/numeric strings, invalid UUID, extra fields и invalid profile — 422. Не нужно, чтобы клиент передавал оба поля: после resolution **Portal обязан передавать оба**.

Порядок одного create:

1. Нормализовать full run UUID для cache; вызвать `get_export_selection(run, execution_id, attempt=request_attempt, calcjob_uuid=request_uuid)` **ровно один раз**, до cache lookup, spool allocation и scheduling.
2. Проверить response shape/types; сохранить detached scalar copy S. Вызвать `list_execution_artifacts(run, execution_id, attempt=S.attempt, calcjob_uuid=S.calcjob_uuid)`. Проверить каждую row на точное совпадение всех трёх identity fields с S и existing path/size/duplicates bounds. Foreign/mismatched producer row — 422 до preparation/payload, без фильтрации «подходящих» rows.
3. Только теперь искать cache по exact `(canonical_run_uuid, execution_id, S.attempt, S.calcjob_uuid, profile)`. Reuse допускается лишь для unexpired preparing/ready entry с тем же frozen `status`. Status equality — дополнительная проверка пригодности entry, не расширение selection authority.
4. Если key совпал, но freshly observed status отличается: старый ready export сохраняется до TTL для уже выданного export ID; для текущего create требуется новая preparation и новый export ID. Если worker занят старой preparation, вернуть existing `ExportBusy`/409; не создавать второй worker и не переключать status старого entry. Expired/failed cache не reusable. Storage reservation/128-entry/TTL limits учитывают оба retained entries.
5. Новый entry содержит existing fields плюс `attempt`, canonical `calcjob_uuid` и frozen `status`; `execution_id` уже существовал. Эти поля публичны через create/get, `state` продолжает означать preparation state. Authorize/download продолжают привязываться к этому immutable `export_id`; authorize response сохраняет existing URL/expiry shape. `_prepare` получает frozen binding и inventory, а не request defaults.
6. В worker вызвать `get_export_plan(run, execution_id, attempt=S.attempt, calcjob_uuid=S.calcjob_uuid)` до source-copy; затем каждый `open_artifact` с теми же обоими selectors. Если применяется `get_artifact_metadata`, туда передаются те же оба selectors. Статус берётся из S; `get_run` и повторный `get_export_selection` запрещены в preparation. Missing pinned child/metadata или stream failure завершают именно эту preparation как failed, без alternate child.
7. Вызвать Results с exact selected frozen bytes/source и `job_status=S.status`. Existing size-copy check, spool/reserve checks, receipt-tail reconciliation, error cleanup, native download/capability и fixed light theme сохраняются. Frontend отображает returned attempt/UUID/status и передаёт выбранные selectors при явном выборе попытки.

Cache только process-local. Upgrade требует штатного restart Portal, при котором старый all-attempt cache не переносится; никакого read-through legacy key `(run,execution,profile)` и динамического fallback на старый producer нет. Producer additions должны быть доступны до запуска нового consumer; отсутствие registered operation — incompatibility/503, не fallback. Candidate implementation не добавляет persistent cache/schema migration.

## Frozen plan и сохранённая совместимость

Existing `get_export_plan(..., attempt=, calcjob_uuid=)` сохраняет return shape `plan/source/sha256/bytes`, raw-byte preservation, MAX_PLAN_BYTES=16 MiB и full decoded equality с file-backed workflow input. В одном run plan1 и plan2 могут различаться whitespace, serialization и SHA256, но `validate_plan(decode(plan1)) == validate_plan(decode(plan2)) == workflow_input_plan` обязательно. Fixture с разными physical/fingerprint/configuration values не positive exact-child oracle и должен отказывать.

Present retrieved `result/scientific_plan.json` используется буквально. Только отсутствие entry даёт existing fallback `aiida.input.plan` с exact original bytes. Present corrupt/oversized/unreadable/different plan никогда не fallback. Ошибки plan в asynchronous preparation остаются existing `state=failed`/safe error, не превращаются задним числом в HTTP422 после уже выданного 202.

Existing `list_artifacts(run)` остаётся all-attempt listing. Existing single-file metadata/open/authorize/range/capability routes и selectors не меняются. В частности existing mismatched explicit pair остаётся `LookupError`/HTTP404: new composite mismatch422 реализуется только в новых selection/inventory operations и create. Нельзя менять глобальный `_select_child`, чтобы незаметно перевести старые single-file ошибки в 422. Existing one-child composite body без selectors продолжает работать при readable inventory/plan; missing result становится честным unknown вместо прежнего running.

## Error contract

HTTP mappings относятся к synchronous new create admission; AiiDA functions возвращают указанный Python exception, без HTTP зависимости. Exact messages ниже — общий new-operation contract. Backend diagnostics не включают payload/private paths. Существующие `_node` messages и existing worker safe errors сохраняются.

| Условие | Python / message | New create HTTP | Preparation/payload |
| --- | --- | --- | --- |
| Invalid process UUID | `ValueError("A full process UUID is required")` | 422 | Не начинается |
| Invalid execution string | `ValueError("A nonempty execution ID of at most 128 characters is required")` | 422 | Не начинается |
| Invalid attempt | `ValueError("Require a positive attempt")` | 422 | Не начинается |
| Invalid CalcJob UUID | `ValueError("A full CalcJob UUID is required")` | 422 | Не начинается |
| Unknown/wrong-type run | Existing `LookupError("Run not found")` | 404 | Не начинается |
| No exact child / foreign explicit UUID | `LookupError("Selected attempt is unavailable")` | 404 | Не начинается |
| Both explicit selectors: known child UUID of this execution, wrong attempt | `ValueError("Attempt and CalcJob UUID select different children")` | 422 | Не начинается |
| Multiple matches without authoritative record / attempt-only ambiguity | Existing `ValueError("Multiple attempts exist; select an exact attempt or CalcJob UUID")` | 422 | Не начинается |
| Conflicting published records | Existing `ValueError("Conflicting published attempt selections")` | 422 | Не начинается |
| Malformed/foreign/mismatched published record | `ValueError("Published attempt selection is invalid")` | 422 | Не начинается |
| Invalid stored selected UUID/attempt | `ValueError("Selected child identity is invalid")` | 422 | Не начинается |
| Present result metadata is invalid/unreadable/oversized | `ValueError("Selected scientific result metadata is invalid")` | 422 | Не начинается |
| Missing selected retrieval/inventory | `LookupError("Retrieved execution artifact inventory is unavailable")` | 404 | Не начинается |
| Invalid/incomplete/oversized/duplicate-path selected inventory | `ValueError("Selected artifact inventory is invalid or incomplete")` | 422 | Не начинается |
| Complete empty inventory in create | `LookupError("Retrieved execution artifacts are not available")` | 404 | Не начинается |
| Malformed selection response / inventory row identity differs from S | `ValueError("Export selection metadata is inconsistent")` | 422 | Не начинается |
| New producer operation unavailable / actor/profile/backend unavailable outside a selected metadata read | `RuntimeError("Exact export selection service is unavailable")` | 503 | Не начинается; no fallback |
| Busy worker / storage admission | Existing `ExportBusy` / `ExportStorage` | 409 / 507 | Existing semantics |
| Frozen plan failure / pinned child disappears / copy size or I/O failure after 202 | Existing preparation failure handling | GET entry остаётся 200 с `state="failed"` | No ready archive; cleanup сохраняется |

Если present result node или inventory node существует, но его собственное чтение/декодирование невозможно, это invalid selected metadata/422; отсутствие самого output link — missing. Неизвестное состояние AiiDA profile/backend до доступа к конкретному link — service unavailable/503. Raw backend exception нельзя превращать в unknown или successful empty inventory.

## Acceptance для DEV10/DEV11

Это frozen synthetic contract, fixtures **не запускались** в DEV09. Ожидаемые asserts должны различать bytes; mocked successful exporter сам по себе не доказывает identity.

| Case | Oracle/assertion |
| --- | --- |
| Два attempts одного execution, selected2, equal-size различный raw payload | Все selected identity fields равны `(E,2,C2)`; bytes2 equality; ни одного open/read unselected repository; status2, exact raw plan2/source2 |
| Same path и disjoint paths между children | Unselected duplicate path не мешает selected inventory; selected-only list; никакой union, missing path нельзя искать в child1 |
| Published selection меняется на child1 после единственного resolve | Все subsequent inventory/plan/metadata/open calls содержат `attempt=2,calcjob_uuid=C2`; frozen status2; archive bytes2; resolve call count=1 |
| Plan1/plan2 имеют разные bytes/whitespace, decoded equality с workflow input | Selected plan2 bytes/hash/source; mutation decoded fingerprint/configuration всегда reject, no input fallback |
| Different known scientific statuses у children + exit0 у failed child | Selected recorded status только child2; process exit0 не заменяет failed; никакого get_run в worker |
| Missing result output при exit0 и при process failure | `unknown` в selection/entry/Results job_status; nonterminal completeness; node-present bad JSON/missing status/unsupported status/foreign or missing execution ID reject422 |
| Legacy один child, нет selection, нет attempt input | Attempt1 canonical UUID; omitted/attempt-only/UUID-only/both consistent positive; existing one-child HTTP body работает |
| Invalid bool/zero/negative/float/string attempt, invalid UUID | 422 до payload/preparation; no coercion |
| Known UUID of E с wrong explicit attempt / foreign UUID / missing attempt | New mismatch422; foreign/missing404; existing single-file mismatch404 отдельной regression assertion |
| Ambiguity, conflicting/malformed/foreign published selection | 422 до payload; нет latest/single-child fallback при present invalid record |
| Missing retrieved/inventory, incomplete/overlimit/invalid path/bool size/duplicate row | Missing404, invalid422; inventory operations не open/walk/seek/stat/hash repository |
| Cache child2 затем child1 | Разные keys/export IDs; никакого reuse даже при equal sizes/paths/status; canonical UUID spellings дают один key |
| Same child: unknown result затем published completed result | Status фиксирован у старого export; новый create не reuse status-unknown entry; busy409 либо новый export после worker availability; не mutate старый export ID |
| Producer возвращает foreign identity row / отсутствует new operation | 422/503 соответственно до payload; не фильтровать row и не использовать legacy all-attempt fallback |
| Storage/TTL/shutdown/plan-corruption regression | Existing reserve/receipt-tail/cleanup/cancel/download assertions сохраняются; failed prep не становится ready |

Existing assertions сохраняются: AiiDA `test_equal_size_content_oracle_and_selection_pin`, `test_metadata_fail_closed_without_payload`, `test_retrieved_frozen_plan_preserves_exact_bytes_and_closes_reader`; Portal `test_export_forwards_exact_frozen_plan_bytes_and_source` и single-file API error/race tests. DEV10/11 могут обновить fake service signatures/metadata под new API, сохраняя исходную цель assertions. Полный schema/native delivery/scientific solve coverage не заявляется.

## Authoring и границы handoff

После независимого CP-DESIGN этот документ переносится в `components/qcl-negf-aiida/docs/export-selection.md` отдельной component branch `codex/review-closure-export-contract`, `README.md` получает только ссылку. До CP-DESIGN ни один product file не меняется; commit/push ожидают diff review координатора. DEV10 medium реализует producer; DEV11 medium реализует consumer после доступности producer. Ни local code/tests, ни server/import/build/solver этой карточкой не выполнены.

| Утверждение | Свидетельство | Ограничение | Решение |
| --- | --- | --- | --- |
| Current single-file path уже поддерживает exact child pin | AiiDA53e2a205 `service.py::_select_child/get_artifact_metadata/open_artifact`; Portal36837e9 `test_api.py::test_bearer_shortcut_pins_open_after_metadata_and_exact_limit` | Source/fixture assertions прочитаны; execution отсутствует | сохранить |
| Current composite export не имеет общего pin/cache identity | Portal36837e9 `exports.py::ExportStore.create/_prepare`: all-attempt listing, default plan/open, whole-run status, key из трёх значений | Successful mixed production archive не наблюдался | изменить |
| Additive binding решает identity gap без новой science policy | Exact API/error/cache/status contract и acceptance matrix выше | CP-DESIGN reviewer и DEV10/11 implementation ещё требуются | изменить |
| Missing result не доказывает running/completed | AiiDA53e2a205 `parser.py::QCLExecutionParser.parse`; Contracts status/TERMINAL; Resultsfe27a2a `export.py::export_snapshot` | Unknown — export observation, не native scientific result | сохранить |
| End-to-end archive/native/science acceptance завершена | Только source design; runtime receipts отсутствуют | Test/import/build/server/solver не выполнялись | данных недостаточно |
