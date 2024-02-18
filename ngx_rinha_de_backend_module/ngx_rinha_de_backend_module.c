#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <jansson.h>
#include <libpq-fe.h>

enum rinha_status_t {
    RINHA_OK,
    RINHA_BIZ_ERROR,
    RINHA_NOT_FOUND,
    RINHA_UNEXPECTED_ERROR,
};

typedef struct {
    int valor;
    char tipo;
    char descricao[10];
} rinha_tx_input_t;

typedef struct {
    int saldo;
    int limite;
} rinha_tx_output_t;

static char * ngx_rinha_de_backend_extrato(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char * ngx_rinha_de_backend_transacao(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_command_t ngx_rinha_de_backend_commands[] = {
    {
        ngx_string("rinha_de_backend_extrato"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_rinha_de_backend_extrato,
        0,
        0,
        NULL
    },
    {
        ngx_string("rinha_de_backend_transacao"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_rinha_de_backend_transacao,
        0,
        0,
        NULL
    },
        ngx_null_command
};

static ngx_http_module_t ngx_rinha_de_backend_module_ctx = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
};

ngx_module_t ngx_rinha_de_backend_module = {
    NGX_MODULE_V1,
    &ngx_rinha_de_backend_module_ctx,
    ngx_rinha_de_backend_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING

};

/*
A timestamp is returned as an 8-byte big-endian double precision number of
seconds since POSTGRES_EPOCH_DATE.
POSTGRES_EPOCH_DATE is January 1, 2000 (2000-01-01).
*/
#define POSTGRES_EPOCH_TIMESTAMP 946692000

char *pq_timestamp_to_iso8601(char *dst, long long timestamp) {
    ngx_tm_t tm;
    ngx_localtime(timestamp / 1000000 + POSTGRES_EPOCH_TIMESTAMP, &tm);

    ngx_sprintf((u_char *) dst, "%4d-%02d-%02dT%02d:%02d:%02d",
        tm.ngx_tm_year, tm.ngx_tm_mon,
        tm.ngx_tm_mday, tm.ngx_tm_hour,
        tm.ngx_tm_min, tm.ngx_tm_sec);

    return dst;
}

ngx_int_t ngx_http_respond_json(ngx_http_request_t *req, json_t *response) {
    ngx_int_t rc;
    size_t length;
    char *output;
    ngx_buf_t *buf;
    ngx_chain_t *out;

    json_incref(response);

#ifdef NGX_DEBUG
    output = json_dumps(response, JSON_ENSURE_ASCII | JSON_INDENT(2));
#else
    output = json_dumps(response, JSON_COMPACT);
#endif

    req->headers_out.status = NGX_HTTP_OK;
    req->headers_out.content_length_n = length = strlen(output);
    ngx_http_send_header(req);

    buf = ngx_calloc_buf(req->pool);
    out = ngx_alloc_chain_link(req->pool);

    out->buf = buf;
    out->next = NULL;

    buf->pos = (u_char *) output;
    buf->last = (u_char *) output + length;
    buf->memory = 1;
    buf->last_buf = 1;

    rc = ngx_http_output_filter(req, out);
    json_decref(response);
    return rc;
}

void request_body_handler(ngx_http_request_t *r) {
}

json_t *ngx_read_request_body_as_json(ngx_http_request_t *req) {
    ngx_buf_t *input_buffer;
    json_error_t json_error;

    ngx_http_read_client_request_body(req, request_body_handler);

    // TODO: this doesn't seem right and it should probably consider the whole
    // chain instead of the first buffer.
    input_buffer = req->request_body->bufs->buf;
    return json_loadb(
        (char *) input_buffer->pos,
        input_buffer->last - input_buffer->pos,
        0, &json_error
    );
}

int rinha_get_customer_id(ngx_http_request_t *req, ngx_str_t *customer_id) {
    // TODO: Move to named captures
    ngx_uint_t start = req->captures[2];
    ngx_uint_t end = req->captures[3];
    customer_id->data = &req->captures_data[start];
    customer_id->len = end - start;

    return atoi((char *) customer_id->data);
}

static ngx_int_t ngx_rinha_de_backend_extrato_handler(ngx_http_request_t *req) {
    ngx_int_t rc;
    PGconn *conn;
    PGresult *result;
    char customer_id_buffer[10];
    char time_buffer[20];
    const char *params[1] = { customer_id_buffer };
    json_t *statement_object;
    json_t *transaction_array;
    json_t *balance_object;

    if (!(req->method & NGX_HTTP_GET)) {
        return NGX_DECLINED;
    }

    ngx_str_t customer_id;
    rinha_get_customer_id(req, &customer_id);

    conn = PQconnectdb("user=postgres password=123456 host=localhost");
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        ngx_http_finalize_request(req, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return NGX_OK;
    }

    ngx_memzero((void *) customer_id_buffer, sizeof(customer_id_buffer));
    strncpy((char *) customer_id_buffer, (char *) customer_id.data, customer_id.len);

    result = PQexecParams(
        conn,
        "(SELECT valor, 'saldo' AS tipo, 'saldo' AS descricao, now() AS realizada_em, c.limite AS limite\n"
        "FROM saldos\n"
        "INNER JOIN clientes c ON c.id = saldos.cliente_id\n"
        "WHERE cliente_id = $1)\n"
        "UNION ALL\n"
        "(SELECT valor, tipo, descricao, realizada_em, NULL\n "
        "FROM transacoes\n"
        "WHERE cliente_id = $1\n"
        "ORDER BY id DESC LIMIT 10)",
        1,
        NULL,
        params,
        NULL,
        NULL,
        1
    );

    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "failed: %s", PQerrorMessage(conn));
        PQclear(result);
        ngx_http_finalize_request(req, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return NGX_OK;
    }

    if (PQntuples(result) == 0) {
        ngx_http_finalize_request(req, NGX_HTTP_NOT_FOUND);
        return NGX_OK;
    }


    transaction_array = json_array();

    for (int i = 1; i < PQntuples(result); i++) {
        json_t *transaction_object = json_object();

        json_object_set(
            transaction_object,
            "valor",
            json_integer(
                (int) ntohl(*((uint32_t *) PQgetvalue(result, i, 0)))
            )
        );
        json_object_set(
            transaction_object,
            "tipo",
            json_string(PQgetvalue(result, i, 1))
        );
        json_object_set(
            transaction_object,
            "descricao",
            json_string(PQgetvalue(result, i, 2))
        );
        json_object_set(
            transaction_object,
            "realizada_em",
            json_string(
                pq_timestamp_to_iso8601(
                    time_buffer, ntohll(*((uint64_t *) PQgetvalue(result, i, 3)))
                )
            )
        );

        json_array_append(transaction_array, transaction_object);
    }

    balance_object = json_object();
    json_object_set(
        balance_object,
        "total",
        json_integer(
            (int) ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)))
        )
    );
    json_object_set(
        balance_object,
        "data_extrato",
        json_string(
            pq_timestamp_to_iso8601(time_buffer, ntohll(*((uint64_t *) PQgetvalue(result, 0, 3))))
        )
    );
    json_object_set(
        balance_object,
        "limite",
        json_integer(
            (int) ntohl(*((uint32_t *) PQgetvalue(result, 0, 4)))
        )
    );

    statement_object = json_object();
    json_object_set(statement_object, "saldo", balance_object);
    json_object_set(statement_object, "ultimas_transacoes", transaction_array);

    rc = ngx_http_respond_json(req, statement_object);
    json_decref(statement_object);
    return rc;
}

ngx_int_t rinha_read_tx_input(ngx_http_request_t *req, rinha_tx_input_t *tx) {
    json_t *descricao;
    json_t *object;
    json_t *tipo;
    size_t length;

    object = ngx_read_request_body_as_json(req);
    tx->valor = json_integer_value(json_object_get(object, "valor"));

    tipo = json_object_get(object, "tipo");
    tx->tipo = json_string_value(tipo)[0];

    descricao = json_object_get(object, "descricao");
    strncpy(tx->descricao, json_string_value(descricao), 10);

    if (tx->valor < 0) {
        return -1;
    }

    if (json_string_length(tipo) != 1 || !(tx->tipo == 'c' || tx->tipo == 'd')) {
        return -1;
    }

    length = json_string_length(descricao);
    if (length < 1 || length > 10) {
        return -1;
    }

    json_decref(object);
    return 0;
}

enum rinha_status_t rinha_transact_for_customer(
    ngx_str_t *customer_id,
    rinha_tx_input_t *tx,
    rinha_tx_output_t *output
) {
    PGconn *conn;
    PGresult *result;
    char query_params_buffer[4][10];
    const char *query_params[4];

    conn = PQconnectdb("user=postgres password=123456 host=localhost");
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        return RINHA_UNEXPECTED_ERROR;
    }

    memcpy(query_params_buffer[0], (char *) customer_id->data, customer_id->len);
    query_params_buffer[0][customer_id->len] = '\0';

    sprintf(query_params_buffer[1], "%d", tx->tipo == 'c' ? tx->valor : -tx->valor);

    query_params_buffer[2][0] = tx->tipo;
    query_params_buffer[2][1] = '\0';

    strncpy(query_params_buffer[3], tx->descricao, 10);

    query_params[0] = query_params_buffer[0];
    query_params[1] = query_params_buffer[1];
    query_params[2] = query_params_buffer[2];
    query_params[3] = query_params_buffer[3];


    result = PQexec(conn, "BEGIN");
    if (PQresultStatus(result) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(result);
        return RINHA_UNEXPECTED_ERROR;
    }

    result = PQexecParams(
        conn,
        "SELECT limite FROM clientes WHERE id = $1",
        1,
        NULL,
        query_params,
        NULL,
        NULL,
        1
    );
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "failed: %s", PQerrorMessage(conn));
        PQclear(result);
        return RINHA_UNEXPECTED_ERROR;
    }

    if (PQntuples(result) == 0) {
        PQexec(conn, "ROLLBACK");
        PQclear(result);
        return RINHA_NOT_FOUND;
    }
    output->limite = (int) ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)));

    result = PQexecParams(
        conn,
        "INSERT INTO transacoes (cliente_id, valor, tipo, descricao, realizada_em) "
        "VALUES ($1, $2, $3, $4, now())",
        4,
        NULL,
        query_params,
        NULL,
        NULL,
        1
    );

    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        fprintf(stderr, "failed: %s", PQerrorMessage(conn));
        PQexec(conn, "ROLLBACK");
        PQclear(result);
        return RINHA_UNEXPECTED_ERROR;
    }

    result = PQexecParams(
        conn,
        "UPDATE saldos\n"
        "SET valor = valor + $2\n"
        "WHERE cliente_id = $1\n"
        "RETURNING valor AS saldo",
        2,
        NULL,
        query_params,
        NULL,
        NULL,
        1
    );
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        fprintf(stderr, "failed: %s", PQerrorMessage(conn));
        PQexec(conn, "ROLLBACK");
        PQclear(result);
        return RINHA_UNEXPECTED_ERROR;
    }

    output->saldo = (int) ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)));

    if (-output->saldo > output->limite) {
        result = PQexec(conn, "ROLLBACK");
        PQclear(result);
        return RINHA_BIZ_ERROR;
    }

    result = PQexec(conn, "END");
    PQclear(result);
    PQfinish(conn);

    return RINHA_OK;
}

static ngx_int_t ngx_rinha_de_backend_transacao_handler(ngx_http_request_t *req) {
    enum rinha_status_t status;
    ngx_int_t rc;
    ngx_str_t customer_id;
    json_t *output_object;
    rinha_tx_input_t tx;
    rinha_tx_output_t output = {
        .limite = 0,
        .saldo = 0
    };

    if (!(req->method & NGX_HTTP_POST)) {
        return NGX_DECLINED;
    }

    if (rinha_get_customer_id(req, &customer_id) <= 0) {
        ngx_http_finalize_request(req, NGX_HTTP_NOT_FOUND);
        return NGX_OK;
    };

    if (rinha_read_tx_input(req, &tx) < 0) {
        ngx_http_finalize_request(req, 422);
        return NGX_OK;
    };

    status = rinha_transact_for_customer(&customer_id, &tx, &output);
    switch (status) {
        case RINHA_OK:
            break;
        case RINHA_BIZ_ERROR:
            ngx_http_finalize_request(req, 422);
            return NGX_OK;
        case RINHA_NOT_FOUND:
            ngx_http_finalize_request(req, NGX_HTTP_NOT_FOUND);
            return NGX_OK;
        case RINHA_UNEXPECTED_ERROR:
            ngx_http_finalize_request(req, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return NGX_OK;
        default:
            break;
    }

    output_object = json_object();
    json_object_set_new(
        output_object,
        "limite",
        json_integer(output.limite)
    );
    json_object_set_new(
        output_object,
        "saldo",
        json_integer(output.saldo)
    );

    rc = ngx_http_respond_json(req, output_object);
    json_decref(output_object);
    return rc;
}


static char *ngx_rinha_de_backend_extrato(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_http_core_loc_conf_t *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_rinha_de_backend_extrato_handler;
    return NGX_CONF_OK;
}

static char *ngx_rinha_de_backend_transacao(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_http_core_loc_conf_t *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_rinha_de_backend_transacao_handler;
    return NGX_CONF_OK;
}