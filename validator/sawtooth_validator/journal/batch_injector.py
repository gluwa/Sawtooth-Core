# Copyright 2017 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

import abc
import hashlib
import importlib
import uuid

from sawtooth_validator.state.settings_view import SettingsView

from sawtooth_validator.protobuf.batch_pb2 import Batch
from sawtooth_validator.protobuf.batch_pb2 import BatchHeader
from sawtooth_validator.protobuf.transaction_pb2 import Transaction
from sawtooth_validator.protobuf.transaction_pb2 import TransactionHeader


class BatchInjectorFactory(metaclass=abc.ABCMeta):
    """The interface to implement for constructing batch injectors"""

    @abc.abstractmethod
    def create_injectors(self, previous_block_id):
        """
        Instantiate all batch injectors that are enabled.

        Returns:
            A list of BatchInjectors.
        """
        raise NotImplementedError()


class BatchInjector(metaclass=abc.ABCMeta):
    """The interface to implement for injecting batches during block
    publishing."""

    @property
    def version():
        raise NotImplementedError()

    @property
    def identifier():
        raise NotImplementedError()

    @abc.abstractmethod
    def block_start(self, previous_block):
        """Returns an ordered list of batches to inject at the beginning of the
        block. Can also return None if no batches should be injected.

        Args:
            previous_block (Block): The previous block.

        Returns:
            A list of batches to inject.
        """
        raise NotImplementedError()


class UnknownBatchInjectorError(Exception):
    def __init__(self, injector_name):
        super().__init__("Unknown injector: %s" % injector_name)


class DefaultBatchInjectorFactory:
    def __init__(self, state_view_factory, signer):
        """
        Args:
            state_view_factory (:obj:`StateViewFactory`): The state view
                factory, for passing to injectors that require it.
            signer (:obj:`Signer`): The cryptographic signer of the validator.
        """
        self._state_view_factory = state_view_factory
        self._signer = signer

    def _read_injector_setting(self, state_root_hash):
        state_view = self._state_view_factory.create_view(state_root_hash)
        settings_view = SettingsView(state_view)
        batch_injector_setting = settings_view.get_setting(
            "sawtooth.validator.batch_injectors")
        return [] if not batch_injector_setting \
            else batch_injector_setting.split(',')

    def create_injectors(self, state_root_hash):
        #injectors = self._read_injector_setting(state_root_hash)
        # return [self._create_injector(i) for i in injectors]
        return [GluwaBatchInjector(self._signer)]

    def _create_injector(self, injector):
        """Returns a new batch injector"""
        if injector == "block_info":
            from sawtooth_validator.journal.block_info_injector import BlockInfoInjector
            return BlockInfoInjector(self._state_view_factory, self._signer)
        elif injector == "gluwa":
            from sawtooth_validator.journal.gluwa_injector import GluwaBatchInjector
            return GluwaBatchInjector(self._signer)

        raise UnknownBatchInjectorError(injector)


class GluwaBatchInjector(BatchInjector):
    housekeeping_payload = b'\xa2avlHousekeepingbp1a0'

    def __init__(self, signer):
        self._signer = signer

    def block_start(self, previous_block):
        pub_key = self._signer.get_public_key().as_hex()
        version = '1.8'
        ns = '8a1a04'
        payload = GluwaBatchInjector.housekeeping_payload
        tx_header = TransactionHeader(
            family_name='CREDITCOIN',
            family_version=version,
            inputs=[ns],
            outputs=[ns],
            nonce=str(uuid.uuid4()),
            batcher_public_key=pub_key,
            dependencies=[],
            signer_public_key=pub_key,
            payload_sha512=hashlib.sha512(payload).hexdigest()
        )
        tx_header_str = tx_header.SerializeToString()
        tx_header_signature = self._signer.sign(tx_header_str)
        tx = Transaction(
            payload=payload,
            header=tx_header_str,
            header_signature=tx_header_signature
        )
        txn_signatures = [tx.header_signature]
        batch_header = BatchHeader(
            signer_public_key=pub_key,
            transaction_ids=txn_signatures
        )
        batch_header_str = batch_header.SerializeToString()
        batch_header_signature = self._signer.sign(batch_header_str)
        batch = Batch(
            header=batch_header_str,
            transactions=[tx],
            header_signature=batch_header_signature
        )
        return [batch]

    def version():
        return "0.1"

    def identifier():
        return "GluwaHousekeeping"
