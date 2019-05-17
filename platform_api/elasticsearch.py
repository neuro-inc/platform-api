from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional, Sequence

import elasticsearch.client.utils
from aioelasticsearch import Elasticsearch as _Elasticsearch
from aiohttp import BasicAuth
from async_generator import asynccontextmanager


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]
    user: Optional[str] = None
    password: Optional[str] = None


class Elasticsearch(_Elasticsearch):
    # NOTE (A Danshyn 05/15/19): this method is a copy of
    # `elasticsearch.client.Elasticsearch.search` (elasticsearch==6.3.1)
    # with the HTTP method changed to POST instead of GET.
    # The reason for this is that Google HTTP LB did not support a body in a
    # GET request.
    # See
    # 1. https://issuetracker.google.com/issues/36886282
    # 2. https://www.elastic.co/guide/en/elasticsearch/reference/6.3
    #    /search-request-body.html#_parameters_4
    # "Both HTTP GET and HTTP POST can be used to execute search with body.
    #  Since not all clients support GET with body, POST is allowed as well."
    @elasticsearch.client.utils.query_params(
        "_source",
        "_source_exclude",
        "_source_include",
        "allow_no_indices",
        "allow_partial_search_results",
        "analyze_wildcard",
        "analyzer",
        "batched_reduce_size",
        "default_operator",
        "df",
        "docvalue_fields",
        "expand_wildcards",
        "explain",
        "from_",
        "ignore_unavailable",
        "lenient",
        "max_concurrent_shard_requests",
        "pre_filter_shard_size",
        "preference",
        "q",
        "request_cache",
        "routing",
        "scroll",
        "search_type",
        "size",
        "sort",
        "stats",
        "stored_fields",
        "suggest_field",
        "suggest_mode",
        "suggest_size",
        "suggest_text",
        "terminate_after",
        "timeout",
        "track_scores",
        "track_total_hits",
        "typed_keys",
        "version",
    )
    def search(
        self,
        index: Optional[str] = None,
        doc_type: Optional[str] = None,
        body: Any = None,
        *,
        params: Dict[str, Any]
    ) -> Any:
        """
        Execute a search query and get back search hits that match the query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html>`_

        :arg index: A comma-separated list of index names to search; use `_all`
            or empty string to perform the operation on all indices
        :arg doc_type: A comma-separated list of document types to search; leave
            empty to perform the operation on all types
        :arg body: The search definition using the Query DSL
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg allow_partial_search_results: Set to false to return an overall
            failure if the request would produce partial results. Defaults to
            True, which will allow partial results in the case of timeouts or
            partial failures
        :arg analyze_wildcard: Specify whether wildcard and prefix queries
            should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg batched_reduce_size: The number of shard results that should be
            reduced at once on the coordinating node. This value should be used
            as a protection mechanism to reduce the memory overhead per search
            request if the potential number of shards in the request can be
            large., default 512
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg docvalue_fields: A comma-separated list of fields to return as the
            docvalue representation of a field for each hit
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg explain: Specify whether to return detailed information about score
            computation as part of a hit
        :arg from\\_: Starting offset (default: 0)
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg max_concurrent_shard_requests: The number of concurrent shard
            requests this search executes concurrently. This value should be
            used to limit the impact of the search on the cluster in order to
            limit the number of concurrent shard requests, default 'The default
            grows with the number of nodes in the cluster but is at most 256.'
        :arg pre_filter_shard_size: A threshold that enforces a pre-filter
            roundtrip to prefilter search shards based on query rewriting if
            the number of shards the search request expands to exceeds the
            threshold. This filter roundtrip can limit the number of shards
            significantly if for instance a shard can not match any documents
            based on it's rewrite method ie. if date filters are mandatory to
            match but the shard bounds and the query are disjoint., default 128
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg request_cache: Specify if request cache should be used for this
            request or not, defaults to index level setting
        :arg routing: A comma-separated list of specific routing values
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :arg search_type: Search operation type, valid choices are:
            'query_then_fetch', 'dfs_query_then_fetch'
        :arg size: Number of hits to return (default: 10)
        :arg sort: A comma-separated list of <field>:<direction> pairs
        :arg stats: Specific 'tag' of the request for logging and statistical
            purposes
        :arg stored_fields: A comma-separated list of stored fields to return as
            part of a hit
        :arg suggest_field: Specify which field to use for suggestions
        :arg suggest_mode: Specify suggest mode, default 'missing', valid
            choices are: 'missing', 'popular', 'always'
        :arg suggest_size: How many suggestions to return in response
        :arg suggest_text: The source text for which the suggestions should be
            returned
        :arg terminate_after: The maximum number of documents to collect for
            each shard, upon reaching which the query execution will terminate
            early.
        :arg timeout: Explicit operation timeout
        :arg track_scores: Whether to calculate and return scores even if they
            are not used for sorting
        :arg track_total_hits: Indicate if the number of documents that match
            the query should be tracked
        :arg typed_keys: Specify whether aggregation and suggester names should
            be prefixed by their respective types in the response
        :arg version: Specify whether to return document version as part of a
            hit
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        if doc_type and not index:
            index = "_all"
        return self.transport.perform_request(
            "POST",
            elasticsearch.client.utils._make_path(index, doc_type, "_search"),
            params=params,
            body=body,
        )

    @elasticsearch.client.utils.query_params("scroll")
    def scroll(
        self,
        scroll_id: Optional[str] = None,
        body: Any = None,
        *,
        params: Dict[str, Any]
    ) -> Any:
        """
        Scroll a search request created by specifying the scroll parameter.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html>`_

        :arg scroll_id: The scroll ID
        :arg body: The scroll ID if not passed by URL or query parameter.
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        """
        if (
            scroll_id in elasticsearch.client.utils.SKIP_IN_PATH
            and body in elasticsearch.client.utils.SKIP_IN_PATH
        ):
            raise ValueError("You need to supply scroll_id or body.")
        elif scroll_id and not body:
            body = {"scroll_id": scroll_id}
        elif scroll_id:
            params["scroll_id"] = scroll_id

        return self.transport.perform_request(
            "POST", "/_search/scroll", params=params, body=body
        )


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig
) -> AsyncIterator[Elasticsearch]:
    http_auth: Optional[BasicAuth]
    if config.user:
        http_auth = BasicAuth(config.user, config.password)  # type: ignore  # noqa
    else:
        http_auth = None

    async with Elasticsearch(hosts=config.hosts, http_auth=http_auth) as es_client:
        yield es_client
