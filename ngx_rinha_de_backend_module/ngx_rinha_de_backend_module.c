#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <jansson.h>
#include <libpq-fe.h>

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

ngx_int_t ngx_http_respond_json(ngx_http_request_t *req, json_t *response) {
    size_t length;
    char *output;
    ngx_buf_t *buf;
    ngx_chain_t *out;

    // TODO: When debug is on keep indent, when off remove it.
    output = json_dumps(response, JSON_ENSURE_ASCII | JSON_INDENT(2));

    req->headers_out.status = NGX_OK;
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

    // TODO: Safely decref from `output`
    return ngx_http_output_filter(req, out);
}

void request_body_handler(ngx_http_request_t *r) {
}

json_t *ngx_read_request_body_as_json(ngx_http_request_t *req) {
    ngx_buf_t *input_buffer;
    json_error_t json_error;

    ngx_http_read_client_request_body(req, request_body_handler);
    // TODO: this doesn't seem right and it should probably conside the whole
    // chain instead of the first buffer.
    input_buffer = req->request_body->bufs->buf;
    return json_loadb(
        (char *) input_buffer->pos,
        input_buffer->last - input_buffer->pos,
        0, &json_error
    );
}

void rinha_get_customer_id(ngx_http_request_t *req, ngx_str_t *customer_id) {
    ngx_uint_t start = req->captures[2];
    ngx_uint_t end = req->captures[3];
    customer_id->data = &req->captures_data[start];
    customer_id->len = end - start;
}

static ngx_int_t ngx_rinha_de_backend_extrato_handler(ngx_http_request_t *req) {
    ngx_int_t rc;
    PGconn *conn;
    PGresult *result;
    char customer_id_buffer[10];
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
        return NGX_ABORT;
        // TODO: return nok / 500?
    }

    ngx_memzero((void *) customer_id_buffer, sizeof(customer_id_buffer));
    strncpy((char *) customer_id_buffer, (char *) customer_id.data, customer_id.len);

    result = PQexecParams(
        conn,
        "(SELECT valor, 'saldo' AS tipo, 'saldo' AS descricao, now()::timestamp AS realizada_em\n"
        "FROM saldos\n"
        "WHERE cliente_id = $1)\n"
        "UNION ALL\n"
        "(SELECT valor, tipo, descricao, realizada_em::timestamp\n "
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
        return NGX_ABORT;
    }


    transaction_array = json_array();

    for (int i = 1; i < PQntuples(result); i++) {
        json_t *transaction_object = json_object();
        ngx_tm_t time;
        ngx_localtime(ntohll(*((uint64_t *) PQgetvalue(result, i, 3))), &time);
        printf("TIMESTAMP %d\n", time.tm_year);
        json_object_set(
            transaction_object,
            "valor",
            json_integer(
                ntohl(*((uint32_t *) PQgetvalue(result, i, 0)))
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
            json_integer(
                ntohll(*((uint64_t *) PQgetvalue(result, i, 3)))
            )
        );

        json_array_append(transaction_array, transaction_object);
    }

    balance_object = json_object();
    json_object_set(
        balance_object,
        "total",
        json_integer(
            ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)))
        )
    );
    json_object_set(
        balance_object,
        "data_extrato",
        json_integer(
            ntohll(*((uint64_t *) PQgetvalue(result, 0, 3)))
        )
    );
    json_object_set(
        balance_object,
        "limite",
        json_integer(
            0 /*ntohl(*((uint32_t *) PQgetvalue(result, 0, 2)))*/
        )
    );

    statement_object = json_object();
    json_object_set(statement_object, "saldo", balance_object);
    json_object_set(statement_object, "ultimas_transacoes", transaction_array);

    rc = ngx_http_respond_json(req, statement_object);
    json_decref(statement_object);
    return rc;
}

void rinha_read_tx_input(ngx_http_request_t *req, rinha_tx_input_t *tx) {
    const char *descricao;
    json_t *object;

    object = ngx_read_request_body_as_json(req);
    tx->valor = json_integer_value(json_object_get(object, "valor"));
    tx->tipo = json_string_value(json_object_get(object, "tipo"))[0];

    descricao = json_string_value(json_object_get(object, "descricao"));
    strncpy(tx->descricao, descricao, 10);
    json_decref(object);
}

void rinha_transact_for_customer(
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
        return;
        // TODO: return nok / 500?
    }

    memcpy(query_params_buffer[0], (char *) customer_id->data, customer_id->len);
    query_params_buffer[0][customer_id->len] = '\0';

    sprintf(query_params_buffer[1], "%d", tx->valor);

    query_params_buffer[2][0] = tx->tipo;
    query_params_buffer[2][1] = '\0';

    strncpy(query_params_buffer[3], tx->descricao, 10);

    query_params[0] = query_params_buffer[0];
    query_params[1] = query_params_buffer[1];
    query_params[2] = query_params_buffer[2];
    query_params[3] = query_params_buffer[3];

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
        return;
    }

    if (PQntuples(result) == 0) {
        PQclear(result);
        output->limite = -1;
        output->saldo = 0;
        // TODO: Return 404?
        return;
    }
    output->limite = ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)));

    result = PQexec(conn, "BEGIN");
    if (PQresultStatus(result) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(result);
        return;
    }

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
        PQclear(result);
        return;
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
        PQclear(result);
        return;
    }

    output->saldo = ntohl(*((uint32_t *) PQgetvalue(result, 0, 0)));

    result = PQexec(conn, "END");
    PQclear(result);
    PQfinish(conn);
}

static ngx_int_t ngx_rinha_de_backend_transacao_handler(ngx_http_request_t *req) {
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

    // TODO: validate input
    rinha_get_customer_id(req, &customer_id);
    rinha_read_tx_input(req, &tx);
    rinha_transact_for_customer(&customer_id, &tx, &output);

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