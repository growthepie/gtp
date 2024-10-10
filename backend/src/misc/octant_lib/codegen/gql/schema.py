import sgqlc.types


schema = sgqlc.types.Schema()


__docformat__ = 'markdown'


########################################################################
# Scalars and Enumerations
########################################################################
class BigDecimal(sgqlc.types.Scalar):
    __schema__ = schema


class BigInt(sgqlc.types.Scalar):
    __schema__ = schema


Boolean = sgqlc.types.Boolean

class Bytes(sgqlc.types.Scalar):
    __schema__ = schema


class Epoch_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `decisionWindow`None
    * `duration`None
    * `epoch`None
    * `fromTs`None
    * `id`None
    * `toTs`None
    '''
    __schema__ = schema
    __choices__ = ('decisionWindow', 'duration', 'epoch', 'fromTs', 'id', 'toTs')


Float = sgqlc.types.Float

ID = sgqlc.types.ID

Int = sgqlc.types.Int

class Int8(sgqlc.types.Scalar):
    '''8 bytes signed integer'''
    __schema__ = schema


class LockedSummaryLatest_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `blockNumber`None
    * `glmSupply`None
    * `id`None
    * `lockedRatio`None
    * `lockedTotal`None
    * `timestamp`None
    * `transactionHash`None
    '''
    __schema__ = schema
    __choices__ = ('blockNumber', 'glmSupply', 'id', 'lockedRatio', 'lockedTotal', 'timestamp', 'transactionHash')


class LockedSummarySnapshot_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `blockNumber`None
    * `glmSupply`None
    * `id`None
    * `lockedRatio`None
    * `lockedTotal`None
    * `timestamp`None
    * `transactionHash`None
    '''
    __schema__ = schema
    __choices__ = ('blockNumber', 'glmSupply', 'id', 'lockedRatio', 'lockedTotal', 'timestamp', 'transactionHash')


class Locked_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `amount`None
    * `blockNumber`None
    * `depositBefore`None
    * `id`None
    * `timestamp`None
    * `transactionHash`None
    * `user`None
    '''
    __schema__ = schema
    __choices__ = ('amount', 'blockNumber', 'depositBefore', 'id', 'timestamp', 'transactionHash', 'user')


class OrderDirection(sgqlc.types.Enum):
    '''Defines the order direction, either ascending or descending

    Enumeration Choices:

    * `asc`None
    * `desc`None
    '''
    __schema__ = schema
    __choices__ = ('asc', 'desc')


class ProjectsMetadataAccumulated_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `id`None
    * `projectsAddresses`None
    '''
    __schema__ = schema
    __choices__ = ('id', 'projectsAddresses')


class ProjectsMetadataPerEpoch_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `epoch`None
    * `id`None
    * `projectsAddresses`None
    * `proposalsCid`None
    '''
    __schema__ = schema
    __choices__ = ('epoch', 'id', 'projectsAddresses', 'proposalsCid')


String = sgqlc.types.String

class Unlocked_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `amount`None
    * `blockNumber`None
    * `depositBefore`None
    * `id`None
    * `timestamp`None
    * `transactionHash`None
    * `user`None
    '''
    __schema__ = schema
    __choices__ = ('amount', 'blockNumber', 'depositBefore', 'id', 'timestamp', 'transactionHash', 'user')


class VaultMerkleRoot_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `blockNumber`None
    * `epoch`None
    * `id`None
    * `root`None
    * `timestamp`None
    * `transactionHash`None
    '''
    __schema__ = schema
    __choices__ = ('blockNumber', 'epoch', 'id', 'root', 'timestamp', 'transactionHash')


class Withdrawal_orderBy(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `amount`None
    * `blockNumber`None
    * `epoch`None
    * `id`None
    * `timestamp`None
    * `transactionHash`None
    * `user`None
    '''
    __schema__ = schema
    __choices__ = ('amount', 'blockNumber', 'epoch', 'id', 'timestamp', 'transactionHash', 'user')


class _SubgraphErrorPolicy_(sgqlc.types.Enum):
    '''Enumeration Choices:

    * `allow`: Data will be returned even if the subgraph has indexing
      errors
    * `deny`: If the subgraph has indexing errors, data will be
      omitted. The default.
    '''
    __schema__ = schema
    __choices__ = ('allow', 'deny')



########################################################################
# Input Objects
########################################################################
class BlockChangedFilter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('number_gte',)
    number_gte = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='number_gte')



class Block_height(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('hash', 'number', 'number_gte')
    hash = sgqlc.types.Field(Bytes, graphql_name='hash')

    number = sgqlc.types.Field(Int, graphql_name='number')

    number_gte = sgqlc.types.Field(Int, graphql_name='number_gte')



class Epoch_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'epoch', 'epoch_not', 'epoch_gt', 'epoch_lt', 'epoch_gte', 'epoch_lte', 'epoch_in', 'epoch_not_in', 'duration', 'duration_not', 'duration_gt', 'duration_lt', 'duration_gte', 'duration_lte', 'duration_in', 'duration_not_in', 'decision_window', 'decision_window_not', 'decision_window_gt', 'decision_window_lt', 'decision_window_gte', 'decision_window_lte', 'decision_window_in', 'decision_window_not_in', 'from_ts', 'from_ts_not', 'from_ts_gt', 'from_ts_lt', 'from_ts_gte', 'from_ts_lte', 'from_ts_in', 'from_ts_not_in', 'to_ts', 'to_ts_not', 'to_ts_gt', 'to_ts_lt', 'to_ts_gte', 'to_ts_lte', 'to_ts_in', 'to_ts_not_in', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    epoch = sgqlc.types.Field(Int, graphql_name='epoch')

    epoch_not = sgqlc.types.Field(Int, graphql_name='epoch_not')

    epoch_gt = sgqlc.types.Field(Int, graphql_name='epoch_gt')

    epoch_lt = sgqlc.types.Field(Int, graphql_name='epoch_lt')

    epoch_gte = sgqlc.types.Field(Int, graphql_name='epoch_gte')

    epoch_lte = sgqlc.types.Field(Int, graphql_name='epoch_lte')

    epoch_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_in')

    epoch_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_not_in')

    duration = sgqlc.types.Field(BigInt, graphql_name='duration')

    duration_not = sgqlc.types.Field(BigInt, graphql_name='duration_not')

    duration_gt = sgqlc.types.Field(BigInt, graphql_name='duration_gt')

    duration_lt = sgqlc.types.Field(BigInt, graphql_name='duration_lt')

    duration_gte = sgqlc.types.Field(BigInt, graphql_name='duration_gte')

    duration_lte = sgqlc.types.Field(BigInt, graphql_name='duration_lte')

    duration_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='duration_in')

    duration_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='duration_not_in')

    decision_window = sgqlc.types.Field(BigInt, graphql_name='decisionWindow')

    decision_window_not = sgqlc.types.Field(BigInt, graphql_name='decisionWindow_not')

    decision_window_gt = sgqlc.types.Field(BigInt, graphql_name='decisionWindow_gt')

    decision_window_lt = sgqlc.types.Field(BigInt, graphql_name='decisionWindow_lt')

    decision_window_gte = sgqlc.types.Field(BigInt, graphql_name='decisionWindow_gte')

    decision_window_lte = sgqlc.types.Field(BigInt, graphql_name='decisionWindow_lte')

    decision_window_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='decisionWindow_in')

    decision_window_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='decisionWindow_not_in')

    from_ts = sgqlc.types.Field(BigInt, graphql_name='fromTs')

    from_ts_not = sgqlc.types.Field(BigInt, graphql_name='fromTs_not')

    from_ts_gt = sgqlc.types.Field(BigInt, graphql_name='fromTs_gt')

    from_ts_lt = sgqlc.types.Field(BigInt, graphql_name='fromTs_lt')

    from_ts_gte = sgqlc.types.Field(BigInt, graphql_name='fromTs_gte')

    from_ts_lte = sgqlc.types.Field(BigInt, graphql_name='fromTs_lte')

    from_ts_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='fromTs_in')

    from_ts_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='fromTs_not_in')

    to_ts = sgqlc.types.Field(BigInt, graphql_name='toTs')

    to_ts_not = sgqlc.types.Field(BigInt, graphql_name='toTs_not')

    to_ts_gt = sgqlc.types.Field(BigInt, graphql_name='toTs_gt')

    to_ts_lt = sgqlc.types.Field(BigInt, graphql_name='toTs_lt')

    to_ts_gte = sgqlc.types.Field(BigInt, graphql_name='toTs_gte')

    to_ts_lte = sgqlc.types.Field(BigInt, graphql_name='toTs_lte')

    to_ts_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='toTs_in')

    to_ts_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='toTs_not_in')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('Epoch_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('Epoch_filter'), graphql_name='or')



class LockedSummaryLatest_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_contains_nocase', 'id_not_contains', 'id_not_contains_nocase', 'id_starts_with', 'id_starts_with_nocase', 'id_not_starts_with', 'id_not_starts_with_nocase', 'id_ends_with', 'id_ends_with_nocase', 'id_not_ends_with', 'id_not_ends_with_nocase', 'glm_supply', 'glm_supply_not', 'glm_supply_gt', 'glm_supply_lt', 'glm_supply_gte', 'glm_supply_lte', 'glm_supply_in', 'glm_supply_not_in', 'locked_total', 'locked_total_not', 'locked_total_gt', 'locked_total_lt', 'locked_total_gte', 'locked_total_lte', 'locked_total_in', 'locked_total_not_in', 'locked_ratio', 'locked_ratio_not', 'locked_ratio_gt', 'locked_ratio_lt', 'locked_ratio_gte', 'locked_ratio_lte', 'locked_ratio_in', 'locked_ratio_not_in', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(String, graphql_name='id')

    id_not = sgqlc.types.Field(String, graphql_name='id_not')

    id_gt = sgqlc.types.Field(String, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(String, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(String, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(String, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(String)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(String)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(String, graphql_name='id_contains')

    id_contains_nocase = sgqlc.types.Field(String, graphql_name='id_contains_nocase')

    id_not_contains = sgqlc.types.Field(String, graphql_name='id_not_contains')

    id_not_contains_nocase = sgqlc.types.Field(String, graphql_name='id_not_contains_nocase')

    id_starts_with = sgqlc.types.Field(String, graphql_name='id_starts_with')

    id_starts_with_nocase = sgqlc.types.Field(String, graphql_name='id_starts_with_nocase')

    id_not_starts_with = sgqlc.types.Field(String, graphql_name='id_not_starts_with')

    id_not_starts_with_nocase = sgqlc.types.Field(String, graphql_name='id_not_starts_with_nocase')

    id_ends_with = sgqlc.types.Field(String, graphql_name='id_ends_with')

    id_ends_with_nocase = sgqlc.types.Field(String, graphql_name='id_ends_with_nocase')

    id_not_ends_with = sgqlc.types.Field(String, graphql_name='id_not_ends_with')

    id_not_ends_with_nocase = sgqlc.types.Field(String, graphql_name='id_not_ends_with_nocase')

    glm_supply = sgqlc.types.Field(BigInt, graphql_name='glmSupply')

    glm_supply_not = sgqlc.types.Field(BigInt, graphql_name='glmSupply_not')

    glm_supply_gt = sgqlc.types.Field(BigInt, graphql_name='glmSupply_gt')

    glm_supply_lt = sgqlc.types.Field(BigInt, graphql_name='glmSupply_lt')

    glm_supply_gte = sgqlc.types.Field(BigInt, graphql_name='glmSupply_gte')

    glm_supply_lte = sgqlc.types.Field(BigInt, graphql_name='glmSupply_lte')

    glm_supply_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='glmSupply_in')

    glm_supply_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='glmSupply_not_in')

    locked_total = sgqlc.types.Field(BigInt, graphql_name='lockedTotal')

    locked_total_not = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_not')

    locked_total_gt = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_gt')

    locked_total_lt = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_lt')

    locked_total_gte = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_gte')

    locked_total_lte = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_lte')

    locked_total_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='lockedTotal_in')

    locked_total_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='lockedTotal_not_in')

    locked_ratio = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio')

    locked_ratio_not = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_not')

    locked_ratio_gt = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_gt')

    locked_ratio_lt = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_lt')

    locked_ratio_gte = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_gte')

    locked_ratio_lte = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_lte')

    locked_ratio_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigDecimal)), graphql_name='lockedRatio_in')

    locked_ratio_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigDecimal)), graphql_name='lockedRatio_not_in')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('LockedSummaryLatest_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('LockedSummaryLatest_filter'), graphql_name='or')



class LockedSummarySnapshot_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'glm_supply', 'glm_supply_not', 'glm_supply_gt', 'glm_supply_lt', 'glm_supply_gte', 'glm_supply_lte', 'glm_supply_in', 'glm_supply_not_in', 'locked_total', 'locked_total_not', 'locked_total_gt', 'locked_total_lt', 'locked_total_gte', 'locked_total_lte', 'locked_total_in', 'locked_total_not_in', 'locked_ratio', 'locked_ratio_not', 'locked_ratio_gt', 'locked_ratio_lt', 'locked_ratio_gte', 'locked_ratio_lte', 'locked_ratio_in', 'locked_ratio_not_in', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    glm_supply = sgqlc.types.Field(BigInt, graphql_name='glmSupply')

    glm_supply_not = sgqlc.types.Field(BigInt, graphql_name='glmSupply_not')

    glm_supply_gt = sgqlc.types.Field(BigInt, graphql_name='glmSupply_gt')

    glm_supply_lt = sgqlc.types.Field(BigInt, graphql_name='glmSupply_lt')

    glm_supply_gte = sgqlc.types.Field(BigInt, graphql_name='glmSupply_gte')

    glm_supply_lte = sgqlc.types.Field(BigInt, graphql_name='glmSupply_lte')

    glm_supply_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='glmSupply_in')

    glm_supply_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='glmSupply_not_in')

    locked_total = sgqlc.types.Field(BigInt, graphql_name='lockedTotal')

    locked_total_not = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_not')

    locked_total_gt = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_gt')

    locked_total_lt = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_lt')

    locked_total_gte = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_gte')

    locked_total_lte = sgqlc.types.Field(BigInt, graphql_name='lockedTotal_lte')

    locked_total_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='lockedTotal_in')

    locked_total_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='lockedTotal_not_in')

    locked_ratio = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio')

    locked_ratio_not = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_not')

    locked_ratio_gt = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_gt')

    locked_ratio_lt = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_lt')

    locked_ratio_gte = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_gte')

    locked_ratio_lte = sgqlc.types.Field(BigDecimal, graphql_name='lockedRatio_lte')

    locked_ratio_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigDecimal)), graphql_name='lockedRatio_in')

    locked_ratio_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigDecimal)), graphql_name='lockedRatio_not_in')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('LockedSummarySnapshot_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('LockedSummarySnapshot_filter'), graphql_name='or')



class Locked_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'deposit_before', 'deposit_before_not', 'deposit_before_gt', 'deposit_before_lt', 'deposit_before_gte', 'deposit_before_lte', 'deposit_before_in', 'deposit_before_not_in', 'amount', 'amount_not', 'amount_gt', 'amount_lt', 'amount_gte', 'amount_lte', 'amount_in', 'amount_not_in', 'user', 'user_not', 'user_gt', 'user_lt', 'user_gte', 'user_lte', 'user_in', 'user_not_in', 'user_contains', 'user_not_contains', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    deposit_before = sgqlc.types.Field(BigInt, graphql_name='depositBefore')

    deposit_before_not = sgqlc.types.Field(BigInt, graphql_name='depositBefore_not')

    deposit_before_gt = sgqlc.types.Field(BigInt, graphql_name='depositBefore_gt')

    deposit_before_lt = sgqlc.types.Field(BigInt, graphql_name='depositBefore_lt')

    deposit_before_gte = sgqlc.types.Field(BigInt, graphql_name='depositBefore_gte')

    deposit_before_lte = sgqlc.types.Field(BigInt, graphql_name='depositBefore_lte')

    deposit_before_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='depositBefore_in')

    deposit_before_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='depositBefore_not_in')

    amount = sgqlc.types.Field(BigInt, graphql_name='amount')

    amount_not = sgqlc.types.Field(BigInt, graphql_name='amount_not')

    amount_gt = sgqlc.types.Field(BigInt, graphql_name='amount_gt')

    amount_lt = sgqlc.types.Field(BigInt, graphql_name='amount_lt')

    amount_gte = sgqlc.types.Field(BigInt, graphql_name='amount_gte')

    amount_lte = sgqlc.types.Field(BigInt, graphql_name='amount_lte')

    amount_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_in')

    amount_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_not_in')

    user = sgqlc.types.Field(Bytes, graphql_name='user')

    user_not = sgqlc.types.Field(Bytes, graphql_name='user_not')

    user_gt = sgqlc.types.Field(Bytes, graphql_name='user_gt')

    user_lt = sgqlc.types.Field(Bytes, graphql_name='user_lt')

    user_gte = sgqlc.types.Field(Bytes, graphql_name='user_gte')

    user_lte = sgqlc.types.Field(Bytes, graphql_name='user_lte')

    user_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_in')

    user_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_not_in')

    user_contains = sgqlc.types.Field(Bytes, graphql_name='user_contains')

    user_not_contains = sgqlc.types.Field(Bytes, graphql_name='user_not_contains')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('Locked_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('Locked_filter'), graphql_name='or')



class ProjectsMetadataAccumulated_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'projects_addresses', 'projects_addresses_not', 'projects_addresses_contains', 'projects_addresses_contains_nocase', 'projects_addresses_not_contains', 'projects_addresses_not_contains_nocase', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    projects_addresses = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses')

    projects_addresses_not = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not')

    projects_addresses_contains = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_contains')

    projects_addresses_contains_nocase = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_contains_nocase')

    projects_addresses_not_contains = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not_contains')

    projects_addresses_not_contains_nocase = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not_contains_nocase')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('ProjectsMetadataAccumulated_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('ProjectsMetadataAccumulated_filter'), graphql_name='or')



class ProjectsMetadataPerEpoch_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'epoch', 'epoch_not', 'epoch_gt', 'epoch_lt', 'epoch_gte', 'epoch_lte', 'epoch_in', 'epoch_not_in', 'proposals_cid', 'proposals_cid_not', 'proposals_cid_gt', 'proposals_cid_lt', 'proposals_cid_gte', 'proposals_cid_lte', 'proposals_cid_in', 'proposals_cid_not_in', 'proposals_cid_contains', 'proposals_cid_contains_nocase', 'proposals_cid_not_contains', 'proposals_cid_not_contains_nocase', 'proposals_cid_starts_with', 'proposals_cid_starts_with_nocase', 'proposals_cid_not_starts_with', 'proposals_cid_not_starts_with_nocase', 'proposals_cid_ends_with', 'proposals_cid_ends_with_nocase', 'proposals_cid_not_ends_with', 'proposals_cid_not_ends_with_nocase', 'projects_addresses', 'projects_addresses_not', 'projects_addresses_contains', 'projects_addresses_contains_nocase', 'projects_addresses_not_contains', 'projects_addresses_not_contains_nocase', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    epoch = sgqlc.types.Field(Int, graphql_name='epoch')

    epoch_not = sgqlc.types.Field(Int, graphql_name='epoch_not')

    epoch_gt = sgqlc.types.Field(Int, graphql_name='epoch_gt')

    epoch_lt = sgqlc.types.Field(Int, graphql_name='epoch_lt')

    epoch_gte = sgqlc.types.Field(Int, graphql_name='epoch_gte')

    epoch_lte = sgqlc.types.Field(Int, graphql_name='epoch_lte')

    epoch_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_in')

    epoch_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_not_in')

    proposals_cid = sgqlc.types.Field(String, graphql_name='proposalsCid')

    proposals_cid_not = sgqlc.types.Field(String, graphql_name='proposalsCid_not')

    proposals_cid_gt = sgqlc.types.Field(String, graphql_name='proposalsCid_gt')

    proposals_cid_lt = sgqlc.types.Field(String, graphql_name='proposalsCid_lt')

    proposals_cid_gte = sgqlc.types.Field(String, graphql_name='proposalsCid_gte')

    proposals_cid_lte = sgqlc.types.Field(String, graphql_name='proposalsCid_lte')

    proposals_cid_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(String)), graphql_name='proposalsCid_in')

    proposals_cid_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(String)), graphql_name='proposalsCid_not_in')

    proposals_cid_contains = sgqlc.types.Field(String, graphql_name='proposalsCid_contains')

    proposals_cid_contains_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_contains_nocase')

    proposals_cid_not_contains = sgqlc.types.Field(String, graphql_name='proposalsCid_not_contains')

    proposals_cid_not_contains_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_not_contains_nocase')

    proposals_cid_starts_with = sgqlc.types.Field(String, graphql_name='proposalsCid_starts_with')

    proposals_cid_starts_with_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_starts_with_nocase')

    proposals_cid_not_starts_with = sgqlc.types.Field(String, graphql_name='proposalsCid_not_starts_with')

    proposals_cid_not_starts_with_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_not_starts_with_nocase')

    proposals_cid_ends_with = sgqlc.types.Field(String, graphql_name='proposalsCid_ends_with')

    proposals_cid_ends_with_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_ends_with_nocase')

    proposals_cid_not_ends_with = sgqlc.types.Field(String, graphql_name='proposalsCid_not_ends_with')

    proposals_cid_not_ends_with_nocase = sgqlc.types.Field(String, graphql_name='proposalsCid_not_ends_with_nocase')

    projects_addresses = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses')

    projects_addresses_not = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not')

    projects_addresses_contains = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_contains')

    projects_addresses_contains_nocase = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_contains_nocase')

    projects_addresses_not_contains = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not_contains')

    projects_addresses_not_contains_nocase = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='projectsAddresses_not_contains_nocase')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('ProjectsMetadataPerEpoch_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('ProjectsMetadataPerEpoch_filter'), graphql_name='or')



class Unlocked_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'deposit_before', 'deposit_before_not', 'deposit_before_gt', 'deposit_before_lt', 'deposit_before_gte', 'deposit_before_lte', 'deposit_before_in', 'deposit_before_not_in', 'amount', 'amount_not', 'amount_gt', 'amount_lt', 'amount_gte', 'amount_lte', 'amount_in', 'amount_not_in', 'user', 'user_not', 'user_gt', 'user_lt', 'user_gte', 'user_lte', 'user_in', 'user_not_in', 'user_contains', 'user_not_contains', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    deposit_before = sgqlc.types.Field(BigInt, graphql_name='depositBefore')

    deposit_before_not = sgqlc.types.Field(BigInt, graphql_name='depositBefore_not')

    deposit_before_gt = sgqlc.types.Field(BigInt, graphql_name='depositBefore_gt')

    deposit_before_lt = sgqlc.types.Field(BigInt, graphql_name='depositBefore_lt')

    deposit_before_gte = sgqlc.types.Field(BigInt, graphql_name='depositBefore_gte')

    deposit_before_lte = sgqlc.types.Field(BigInt, graphql_name='depositBefore_lte')

    deposit_before_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='depositBefore_in')

    deposit_before_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='depositBefore_not_in')

    amount = sgqlc.types.Field(BigInt, graphql_name='amount')

    amount_not = sgqlc.types.Field(BigInt, graphql_name='amount_not')

    amount_gt = sgqlc.types.Field(BigInt, graphql_name='amount_gt')

    amount_lt = sgqlc.types.Field(BigInt, graphql_name='amount_lt')

    amount_gte = sgqlc.types.Field(BigInt, graphql_name='amount_gte')

    amount_lte = sgqlc.types.Field(BigInt, graphql_name='amount_lte')

    amount_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_in')

    amount_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_not_in')

    user = sgqlc.types.Field(Bytes, graphql_name='user')

    user_not = sgqlc.types.Field(Bytes, graphql_name='user_not')

    user_gt = sgqlc.types.Field(Bytes, graphql_name='user_gt')

    user_lt = sgqlc.types.Field(Bytes, graphql_name='user_lt')

    user_gte = sgqlc.types.Field(Bytes, graphql_name='user_gte')

    user_lte = sgqlc.types.Field(Bytes, graphql_name='user_lte')

    user_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_in')

    user_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_not_in')

    user_contains = sgqlc.types.Field(Bytes, graphql_name='user_contains')

    user_not_contains = sgqlc.types.Field(Bytes, graphql_name='user_not_contains')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('Unlocked_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('Unlocked_filter'), graphql_name='or')



class VaultMerkleRoot_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'epoch', 'epoch_not', 'epoch_gt', 'epoch_lt', 'epoch_gte', 'epoch_lte', 'epoch_in', 'epoch_not_in', 'root', 'root_not', 'root_gt', 'root_lt', 'root_gte', 'root_lte', 'root_in', 'root_not_in', 'root_contains', 'root_not_contains', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    epoch = sgqlc.types.Field(Int, graphql_name='epoch')

    epoch_not = sgqlc.types.Field(Int, graphql_name='epoch_not')

    epoch_gt = sgqlc.types.Field(Int, graphql_name='epoch_gt')

    epoch_lt = sgqlc.types.Field(Int, graphql_name='epoch_lt')

    epoch_gte = sgqlc.types.Field(Int, graphql_name='epoch_gte')

    epoch_lte = sgqlc.types.Field(Int, graphql_name='epoch_lte')

    epoch_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_in')

    epoch_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_not_in')

    root = sgqlc.types.Field(Bytes, graphql_name='root')

    root_not = sgqlc.types.Field(Bytes, graphql_name='root_not')

    root_gt = sgqlc.types.Field(Bytes, graphql_name='root_gt')

    root_lt = sgqlc.types.Field(Bytes, graphql_name='root_lt')

    root_gte = sgqlc.types.Field(Bytes, graphql_name='root_gte')

    root_lte = sgqlc.types.Field(Bytes, graphql_name='root_lte')

    root_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='root_in')

    root_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='root_not_in')

    root_contains = sgqlc.types.Field(Bytes, graphql_name='root_contains')

    root_not_contains = sgqlc.types.Field(Bytes, graphql_name='root_not_contains')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('VaultMerkleRoot_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('VaultMerkleRoot_filter'), graphql_name='or')



class Withdrawal_filter(sgqlc.types.Input):
    __schema__ = schema
    __field_names__ = ('id', 'id_not', 'id_gt', 'id_lt', 'id_gte', 'id_lte', 'id_in', 'id_not_in', 'id_contains', 'id_not_contains', 'amount', 'amount_not', 'amount_gt', 'amount_lt', 'amount_gte', 'amount_lte', 'amount_in', 'amount_not_in', 'epoch', 'epoch_not', 'epoch_gt', 'epoch_lt', 'epoch_gte', 'epoch_lte', 'epoch_in', 'epoch_not_in', 'user', 'user_not', 'user_gt', 'user_lt', 'user_gte', 'user_lte', 'user_in', 'user_not_in', 'user_contains', 'user_not_contains', 'block_number', 'block_number_not', 'block_number_gt', 'block_number_lt', 'block_number_gte', 'block_number_lte', 'block_number_in', 'block_number_not_in', 'timestamp', 'timestamp_not', 'timestamp_gt', 'timestamp_lt', 'timestamp_gte', 'timestamp_lte', 'timestamp_in', 'timestamp_not_in', 'transaction_hash', 'transaction_hash_not', 'transaction_hash_gt', 'transaction_hash_lt', 'transaction_hash_gte', 'transaction_hash_lte', 'transaction_hash_in', 'transaction_hash_not_in', 'transaction_hash_contains', 'transaction_hash_not_contains', '_change_block', 'and_', 'or_')
    id = sgqlc.types.Field(Bytes, graphql_name='id')

    id_not = sgqlc.types.Field(Bytes, graphql_name='id_not')

    id_gt = sgqlc.types.Field(Bytes, graphql_name='id_gt')

    id_lt = sgqlc.types.Field(Bytes, graphql_name='id_lt')

    id_gte = sgqlc.types.Field(Bytes, graphql_name='id_gte')

    id_lte = sgqlc.types.Field(Bytes, graphql_name='id_lte')

    id_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_in')

    id_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='id_not_in')

    id_contains = sgqlc.types.Field(Bytes, graphql_name='id_contains')

    id_not_contains = sgqlc.types.Field(Bytes, graphql_name='id_not_contains')

    amount = sgqlc.types.Field(BigInt, graphql_name='amount')

    amount_not = sgqlc.types.Field(BigInt, graphql_name='amount_not')

    amount_gt = sgqlc.types.Field(BigInt, graphql_name='amount_gt')

    amount_lt = sgqlc.types.Field(BigInt, graphql_name='amount_lt')

    amount_gte = sgqlc.types.Field(BigInt, graphql_name='amount_gte')

    amount_lte = sgqlc.types.Field(BigInt, graphql_name='amount_lte')

    amount_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_in')

    amount_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(BigInt)), graphql_name='amount_not_in')

    epoch = sgqlc.types.Field(Int, graphql_name='epoch')

    epoch_not = sgqlc.types.Field(Int, graphql_name='epoch_not')

    epoch_gt = sgqlc.types.Field(Int, graphql_name='epoch_gt')

    epoch_lt = sgqlc.types.Field(Int, graphql_name='epoch_lt')

    epoch_gte = sgqlc.types.Field(Int, graphql_name='epoch_gte')

    epoch_lte = sgqlc.types.Field(Int, graphql_name='epoch_lte')

    epoch_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_in')

    epoch_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='epoch_not_in')

    user = sgqlc.types.Field(Bytes, graphql_name='user')

    user_not = sgqlc.types.Field(Bytes, graphql_name='user_not')

    user_gt = sgqlc.types.Field(Bytes, graphql_name='user_gt')

    user_lt = sgqlc.types.Field(Bytes, graphql_name='user_lt')

    user_gte = sgqlc.types.Field(Bytes, graphql_name='user_gte')

    user_lte = sgqlc.types.Field(Bytes, graphql_name='user_lte')

    user_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_in')

    user_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='user_not_in')

    user_contains = sgqlc.types.Field(Bytes, graphql_name='user_contains')

    user_not_contains = sgqlc.types.Field(Bytes, graphql_name='user_not_contains')

    block_number = sgqlc.types.Field(Int, graphql_name='blockNumber')

    block_number_not = sgqlc.types.Field(Int, graphql_name='blockNumber_not')

    block_number_gt = sgqlc.types.Field(Int, graphql_name='blockNumber_gt')

    block_number_lt = sgqlc.types.Field(Int, graphql_name='blockNumber_lt')

    block_number_gte = sgqlc.types.Field(Int, graphql_name='blockNumber_gte')

    block_number_lte = sgqlc.types.Field(Int, graphql_name='blockNumber_lte')

    block_number_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_in')

    block_number_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='blockNumber_not_in')

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')

    timestamp_not = sgqlc.types.Field(Int, graphql_name='timestamp_not')

    timestamp_gt = sgqlc.types.Field(Int, graphql_name='timestamp_gt')

    timestamp_lt = sgqlc.types.Field(Int, graphql_name='timestamp_lt')

    timestamp_gte = sgqlc.types.Field(Int, graphql_name='timestamp_gte')

    timestamp_lte = sgqlc.types.Field(Int, graphql_name='timestamp_lte')

    timestamp_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_in')

    timestamp_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Int)), graphql_name='timestamp_not_in')

    transaction_hash = sgqlc.types.Field(Bytes, graphql_name='transactionHash')

    transaction_hash_not = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not')

    transaction_hash_gt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gt')

    transaction_hash_lt = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lt')

    transaction_hash_gte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_gte')

    transaction_hash_lte = sgqlc.types.Field(Bytes, graphql_name='transactionHash_lte')

    transaction_hash_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_in')

    transaction_hash_not_in = sgqlc.types.Field(sgqlc.types.list_of(sgqlc.types.non_null(Bytes)), graphql_name='transactionHash_not_in')

    transaction_hash_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_contains')

    transaction_hash_not_contains = sgqlc.types.Field(Bytes, graphql_name='transactionHash_not_contains')

    _change_block = sgqlc.types.Field(BlockChangedFilter, graphql_name='_change_block')
    '''Filter for the block changed event.'''

    and_ = sgqlc.types.Field(sgqlc.types.list_of('Withdrawal_filter'), graphql_name='and')

    or_ = sgqlc.types.Field(sgqlc.types.list_of('Withdrawal_filter'), graphql_name='or')




########################################################################
# Output Objects and Interfaces
########################################################################
class Epoch(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'epoch', 'duration', 'decision_window', 'from_ts', 'to_ts')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    epoch = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='epoch')

    duration = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='duration')

    decision_window = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='decisionWindow')

    from_ts = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='fromTs')

    to_ts = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='toTs')



class Locked(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'deposit_before', 'amount', 'user', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    deposit_before = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='depositBefore')

    amount = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='amount')

    user = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='user')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class LockedSummaryLatest(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'glm_supply', 'locked_total', 'locked_ratio', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(String), graphql_name='id')

    glm_supply = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='glmSupply')

    locked_total = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='lockedTotal')

    locked_ratio = sgqlc.types.Field(sgqlc.types.non_null(BigDecimal), graphql_name='lockedRatio')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class LockedSummarySnapshot(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'glm_supply', 'locked_total', 'locked_ratio', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    glm_supply = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='glmSupply')

    locked_total = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='lockedTotal')

    locked_ratio = sgqlc.types.Field(sgqlc.types.non_null(BigDecimal), graphql_name='lockedRatio')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class ProjectsMetadataAccumulated(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'projects_addresses')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    projects_addresses = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Bytes))), graphql_name='projectsAddresses')



class ProjectsMetadataPerEpoch(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'epoch', 'proposals_cid', 'projects_addresses')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    epoch = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='epoch')

    proposals_cid = sgqlc.types.Field(sgqlc.types.non_null(String), graphql_name='proposalsCid')

    projects_addresses = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Bytes))), graphql_name='projectsAddresses')



class Query(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('locked', 'lockeds', 'unlocked', 'unlockeds', 'withdrawal', 'withdrawals', 'vault_merkle_root', 'vault_merkle_roots', 'locked_summary_latest', 'locked_summary_latests', 'locked_summary_snapshot', 'locked_summary_snapshots', 'epoch', 'epoches', 'projects_metadata_per_epoch', 'projects_metadata_per_epoches', 'projects_metadata_accumulated', 'projects_metadata_accumulateds', '_meta')
    locked = sgqlc.types.Field(Locked, graphql_name='locked', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    lockeds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Locked))), graphql_name='lockeds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Locked_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Locked_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Locked_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Locked_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    unlocked = sgqlc.types.Field('Unlocked', graphql_name='unlocked', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    unlockeds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('Unlocked'))), graphql_name='unlockeds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Unlocked_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Unlocked_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Unlocked_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Unlocked_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    withdrawal = sgqlc.types.Field('Withdrawal', graphql_name='withdrawal', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    withdrawals = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('Withdrawal'))), graphql_name='withdrawals', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Withdrawal_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Withdrawal_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Withdrawal_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Withdrawal_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    vault_merkle_root = sgqlc.types.Field('VaultMerkleRoot', graphql_name='vaultMerkleRoot', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    vault_merkle_roots = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('VaultMerkleRoot'))), graphql_name='vaultMerkleRoots', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(VaultMerkleRoot_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(VaultMerkleRoot_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`VaultMerkleRoot_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`VaultMerkleRoot_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_latest = sgqlc.types.Field(LockedSummaryLatest, graphql_name='lockedSummaryLatest', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_latests = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(LockedSummaryLatest))), graphql_name='lockedSummaryLatests', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(LockedSummaryLatest_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(LockedSummaryLatest_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`LockedSummaryLatest_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`LockedSummaryLatest_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_snapshot = sgqlc.types.Field(LockedSummarySnapshot, graphql_name='lockedSummarySnapshot', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_snapshots = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(LockedSummarySnapshot))), graphql_name='lockedSummarySnapshots', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(LockedSummarySnapshot_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(LockedSummarySnapshot_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`LockedSummarySnapshot_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`LockedSummarySnapshot_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    epoch = sgqlc.types.Field(Epoch, graphql_name='epoch', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    epoches = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Epoch))), graphql_name='epoches', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Epoch_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Epoch_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Epoch_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Epoch_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_per_epoch = sgqlc.types.Field(ProjectsMetadataPerEpoch, graphql_name='projectsMetadataPerEpoch', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_per_epoches = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(ProjectsMetadataPerEpoch))), graphql_name='projectsMetadataPerEpoches', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(ProjectsMetadataPerEpoch_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(ProjectsMetadataPerEpoch_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`ProjectsMetadataPerEpoch_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`ProjectsMetadataPerEpoch_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_accumulated = sgqlc.types.Field(ProjectsMetadataAccumulated, graphql_name='projectsMetadataAccumulated', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_accumulateds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(ProjectsMetadataAccumulated))), graphql_name='projectsMetadataAccumulateds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(ProjectsMetadataAccumulated_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(ProjectsMetadataAccumulated_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`ProjectsMetadataAccumulated_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`ProjectsMetadataAccumulated_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    _meta = sgqlc.types.Field('_Meta_', graphql_name='_meta', args=sgqlc.types.ArgDict((
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
))
    )
    '''Access to subgraph metadata

    Arguments:

    * `block` (`Block_height`)None
    '''



class Subscription(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('locked', 'lockeds', 'unlocked', 'unlockeds', 'withdrawal', 'withdrawals', 'vault_merkle_root', 'vault_merkle_roots', 'locked_summary_latest', 'locked_summary_latests', 'locked_summary_snapshot', 'locked_summary_snapshots', 'epoch', 'epoches', 'projects_metadata_per_epoch', 'projects_metadata_per_epoches', 'projects_metadata_accumulated', 'projects_metadata_accumulateds', '_meta')
    locked = sgqlc.types.Field(Locked, graphql_name='locked', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    lockeds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Locked))), graphql_name='lockeds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Locked_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Locked_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Locked_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Locked_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    unlocked = sgqlc.types.Field('Unlocked', graphql_name='unlocked', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    unlockeds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('Unlocked'))), graphql_name='unlockeds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Unlocked_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Unlocked_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Unlocked_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Unlocked_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    withdrawal = sgqlc.types.Field('Withdrawal', graphql_name='withdrawal', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    withdrawals = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('Withdrawal'))), graphql_name='withdrawals', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Withdrawal_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Withdrawal_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Withdrawal_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Withdrawal_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    vault_merkle_root = sgqlc.types.Field('VaultMerkleRoot', graphql_name='vaultMerkleRoot', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    vault_merkle_roots = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null('VaultMerkleRoot'))), graphql_name='vaultMerkleRoots', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(VaultMerkleRoot_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(VaultMerkleRoot_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`VaultMerkleRoot_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`VaultMerkleRoot_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_latest = sgqlc.types.Field(LockedSummaryLatest, graphql_name='lockedSummaryLatest', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_latests = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(LockedSummaryLatest))), graphql_name='lockedSummaryLatests', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(LockedSummaryLatest_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(LockedSummaryLatest_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`LockedSummaryLatest_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`LockedSummaryLatest_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_snapshot = sgqlc.types.Field(LockedSummarySnapshot, graphql_name='lockedSummarySnapshot', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    locked_summary_snapshots = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(LockedSummarySnapshot))), graphql_name='lockedSummarySnapshots', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(LockedSummarySnapshot_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(LockedSummarySnapshot_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`LockedSummarySnapshot_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`LockedSummarySnapshot_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    epoch = sgqlc.types.Field(Epoch, graphql_name='epoch', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    epoches = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(Epoch))), graphql_name='epoches', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(Epoch_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(Epoch_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`Epoch_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`Epoch_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_per_epoch = sgqlc.types.Field(ProjectsMetadataPerEpoch, graphql_name='projectsMetadataPerEpoch', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_per_epoches = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(ProjectsMetadataPerEpoch))), graphql_name='projectsMetadataPerEpoches', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(ProjectsMetadataPerEpoch_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(ProjectsMetadataPerEpoch_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`ProjectsMetadataPerEpoch_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`ProjectsMetadataPerEpoch_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_accumulated = sgqlc.types.Field(ProjectsMetadataAccumulated, graphql_name='projectsMetadataAccumulated', args=sgqlc.types.ArgDict((
        ('id', sgqlc.types.Arg(sgqlc.types.non_null(ID), graphql_name='id', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `id` (`ID!`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    projects_metadata_accumulateds = sgqlc.types.Field(sgqlc.types.non_null(sgqlc.types.list_of(sgqlc.types.non_null(ProjectsMetadataAccumulated))), graphql_name='projectsMetadataAccumulateds', args=sgqlc.types.ArgDict((
        ('skip', sgqlc.types.Arg(Int, graphql_name='skip', default=0)),
        ('first', sgqlc.types.Arg(Int, graphql_name='first', default=100)),
        ('order_by', sgqlc.types.Arg(ProjectsMetadataAccumulated_orderBy, graphql_name='orderBy', default=None)),
        ('order_direction', sgqlc.types.Arg(OrderDirection, graphql_name='orderDirection', default=None)),
        ('where', sgqlc.types.Arg(ProjectsMetadataAccumulated_filter, graphql_name='where', default=None)),
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
        ('subgraph_error', sgqlc.types.Arg(sgqlc.types.non_null(_SubgraphErrorPolicy_), graphql_name='subgraphError', default='deny')),
))
    )
    '''Arguments:

    * `skip` (`Int`)None (default: `0`)
    * `first` (`Int`)None (default: `100`)
    * `order_by` (`ProjectsMetadataAccumulated_orderBy`)None
    * `order_direction` (`OrderDirection`)None
    * `where` (`ProjectsMetadataAccumulated_filter`)None
    * `block` (`Block_height`): The block at which the query should be
      executed. Can either be a `{ hash: Bytes }` value containing a
      block hash, a `{ number: Int }` containing the block number, or
      a `{ number_gte: Int }` containing the minimum block number. In
      the case of `number_gte`, the query will be executed on the
      latest block only if the subgraph has progressed to or past the
      minimum block number. Defaults to the latest block when omitted.
    * `subgraph_error` (`_SubgraphErrorPolicy_!`): Set to `allow` to
      receive data even if the subgraph has skipped over errors while
      syncing. (default: `deny`)
    '''

    _meta = sgqlc.types.Field('_Meta_', graphql_name='_meta', args=sgqlc.types.ArgDict((
        ('block', sgqlc.types.Arg(Block_height, graphql_name='block', default=None)),
))
    )
    '''Access to subgraph metadata

    Arguments:

    * `block` (`Block_height`)None
    '''



class Unlocked(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'deposit_before', 'amount', 'user', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    deposit_before = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='depositBefore')

    amount = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='amount')

    user = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='user')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class VaultMerkleRoot(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'epoch', 'root', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    epoch = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='epoch')

    root = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='root')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class Withdrawal(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('id', 'amount', 'epoch', 'user', 'block_number', 'timestamp', 'transaction_hash')
    id = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='id')

    amount = sgqlc.types.Field(sgqlc.types.non_null(BigInt), graphql_name='amount')

    epoch = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='epoch')

    user = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='user')

    block_number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='blockNumber')

    timestamp = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='timestamp')

    transaction_hash = sgqlc.types.Field(sgqlc.types.non_null(Bytes), graphql_name='transactionHash')



class _Block_(sgqlc.types.Type):
    __schema__ = schema
    __field_names__ = ('hash', 'number', 'timestamp')
    hash = sgqlc.types.Field(Bytes, graphql_name='hash')
    '''The hash of the block'''

    number = sgqlc.types.Field(sgqlc.types.non_null(Int), graphql_name='number')
    '''The block number'''

    timestamp = sgqlc.types.Field(Int, graphql_name='timestamp')
    '''Integer representation of the timestamp stored in blocks for the
    chain
    '''



class _Meta_(sgqlc.types.Type):
    '''The type for the top-level _meta field'''
    __schema__ = schema
    __field_names__ = ('block', 'deployment', 'has_indexing_errors')
    block = sgqlc.types.Field(sgqlc.types.non_null(_Block_), graphql_name='block')
    '''Information about a specific subgraph block. The hash of the block
    will be null if the _meta field has a block constraint that asks
    for a block number. It will be filled if the _meta field has no
    block constraint and therefore asks for the latest  block
    '''

    deployment = sgqlc.types.Field(sgqlc.types.non_null(String), graphql_name='deployment')
    '''The deployment ID'''

    has_indexing_errors = sgqlc.types.Field(sgqlc.types.non_null(Boolean), graphql_name='hasIndexingErrors')
    '''If `true`, the subgraph encountered indexing errors at some past
    block
    '''




########################################################################
# Unions
########################################################################

########################################################################
# Schema Entry Points
########################################################################
schema.query_type = Query
schema.mutation_type = None
schema.subscription_type = Subscription

