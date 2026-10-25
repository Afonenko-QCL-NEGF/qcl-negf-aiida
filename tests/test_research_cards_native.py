"""One explicitly isolated SQLite persistence control; no daemon or processes.

Run separately with plugin autoload disabled and the prepared dependency graph.
Creates exactly ten metadata nodes, then unloads/reopens that same test storage.
This source fixture is not scientific or browser acceptance.
"""
import hashlib
import io
import os
from pathlib import Path
from uuid import UUID

import pytest
from aiida import orm
from aiida.common.exceptions import ModificationNotAllowed
from aiida.manage import get_manager
from aiida.storage.sqlite_dos.backend import SqliteDosStorage

from aiida_qcl_negf import service

RAW = (
    ' {\n"schema":"qcl-negf-research-card-v1","title":"Native",'
    '"goal":" Compare A/B ","question":"Почему?\\nNegative results remain."}\n'
).encode()


def test_native_card_bytes_immutability_numeric_head_and_replay_after_reload(
    aiida_config_factory, aiida_profile_factory, tmp_path,
):
    """Exercise real SinglefileData, repository, query/cast and closed storage."""
    manager = get_manager()
    # Refuse to touch or restore an already loaded user/live profile. The
    # explicitly selected factory below supplies both config and storage paths.
    assert manager.get_profile() is None, 'Run this control alone with no loaded profile'
    storage_path = (tmp_path / 'native-card-storage').resolve()
    with aiida_config_factory(tmp_path / 'native-card-config') as config:
        assert Path(config.dirpath).resolve().is_relative_to(tmp_path.resolve())
        assert Path(os.environ['AIIDA_PATH']).resolve() == Path(config.dirpath).resolve()
        with aiida_profile_factory(
            config, name='qcl-card-native-control', storage_backend='core.sqlite_dos',
            storage_config={'filepath': str(storage_path)}, broker_backend=None,
        ) as profile:
            assert profile.is_test_profile
            assert profile.storage_backend == 'core.sqlite_dos'
            assert profile.process_control_backend is None
            assert Path(profile.storage_config['filepath']).resolve() == storage_path
            backend = manager.get_profile_storage()
            assert isinstance(backend, SqliteDosStorage)
            assert orm.QueryBuilder().append(orm.Node).count() == 0

            requests = [str(UUID(int=1000 + n)) for n in range(1, 11)]
            first = service.create_research_card(requests[0], RAW)
            assert first['status'] == 'stored' and first['kind'] == 'version'
            initial = first['receipt']
            card_uuid = initial['card_uuid']
            assert initial['version_uuid'] == card_uuid
            assert initial['version'] == 1 and initial['previous_version_uuid'] is None
            assert initial['bytes'] == len(RAW)
            assert initial['sha256'] == hashlib.sha256(RAW).hexdigest()
            assert str(UUID(card_uuid)) == card_uuid

            root = orm.load_node(card_uuid)
            assert isinstance(root, orm.SinglefileData)
            assert root.filename == 'research-card.json'
            with pytest.raises(ModificationNotAllowed):
                root.base.attributes.set('title', 'forbidden overwrite')
            with pytest.raises(ModificationNotAllowed):
                root.set_file(io.BytesIO(b'forbidden overwrite'), filename='research-card.json')
            assert service.read_research_card_version(card_uuid, card_uuid, raw=True) == RAW
            del root

            receipts, payloads = [initial], [RAW]
            for version in range(2, 11):
                raw = RAW + b' ' * version  # Literal exact-byte distinction, no normalization.
                result = service.edit_research_card(
                    card_uuid, requests[version - 1], receipts[-1]['version_uuid'], raw,
                )
                assert result['status'] == 'stored' and result['kind'] == 'version'
                receipt = result['receipt']
                assert receipt['version'] == version
                assert receipt['previous_version_uuid'] == receipts[-1]['version_uuid']
                assert receipt['card_uuid'] == card_uuid
                receipts.append(receipt)
                payloads.append(raw)
            assert len({r['version_uuid'] for r in receipts}) == 10
            assert orm.QueryBuilder().append(orm.Node).count() == 10
            # Real native JSON integer ordering must distinguish 10 > 9 > 2.
            assert [r['version'] for r in service.list_research_card_versions(card_uuid)] == [10,9,8,7,6,5,4,3,2,1]
            assert service.get_research_card(card_uuid)['head'] == receipts[-1]

            manager.unload_profile()  # Closes SQLite/repository resources, not a storage reset.
            assert manager.get_profile() is None and not manager.profile_storage_loaded
            reopened = manager.load_profile(profile.name)  # Exact temporary config/profile name.
            assert reopened.uuid == profile.uuid
            assert manager.get_profile_storage() is not backend
            assert isinstance(manager.get_profile_storage(), SqliteDosStorage)

            # Fresh ORM lookups only: no old node/handle supplies persistence proof.
            assert service.get_research_card(card_uuid)['head'] == receipts[-1]
            assert service.list_research_cards() == [receipts[-1]]
            assert [r['version'] for r in service.list_research_card_versions(card_uuid)] == [10,9,8,7,6,5,4,3,2,1]
            for receipt, raw in zip(receipts, payloads, strict=True):
                assert service.read_research_card_version(card_uuid, receipt['version_uuid'], raw=True) == raw
                assert receipt['bytes'] == len(raw)
                assert receipt['sha256'] == hashlib.sha256(raw).hexdigest()
            original = {'status': 'stored', 'kind': 'version', 'receipt': initial}
            assert service.get_research_card_request(requests[0]) == original
            assert service.create_research_card(requests[0], RAW) == {**original, 'status': 'replayed'}
            assert service.edit_research_card(
                card_uuid, requests[-1], receipts[-2]['version_uuid'], payloads[-1],
            ) == {'status': 'replayed', 'kind': 'version', 'receipt': receipts[-1]}
            assert service.create_research_card(requests[0], RAW + b'\n') == {
                'status': 'rejected', 'reason': 'request_conflict',
            }
            assert service.edit_research_card(card_uuid, str(UUID(int=2000)), card_uuid, RAW) == {
                'status': 'rejected', 'reason': 'version_conflict',
                'current_version_uuid': receipts[-1]['version_uuid'],
            }
            assert orm.QueryBuilder().append(orm.Node).count() == 10
            assert orm.QueryBuilder().append(orm.ProcessNode).count() == 0
