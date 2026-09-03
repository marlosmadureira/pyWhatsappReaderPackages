<?php
$db = null;
include_once('funcao.php');

$tokenAuthorized = "2021010251552-Chupisco2";

setlocale(LC_TIME, 'pt_BR', 'pt_BR.utf-8', 'pt_BR.utf-8', 'portuguese');
date_default_timezone_set('America/Sao_Paulo');
mb_internal_encoding("UTF-8");

set_time_limit(0);
ini_set('memory_limit', '-1');


header('Content-Type: application/json; charset=utf-8');
@ini_set('display_errors', '0'); // evita vazar warning no body
error_reporting(E_ALL);

$GLOBALS['__api_sent'] = false;

function api_send($httpCode, $payload) {
    if ($GLOBALS['__api_sent']) { return; }
    $GLOBALS['__api_sent'] = true;

    if (ob_get_length()) { @ob_clean(); }
    http_response_code($httpCode);

    // garante campos mínimos
    if (!isset($payload['ok'])) $payload['ok'] = ($httpCode >= 200 && $httpCode < 300);
    if (!isset($payload['ts_utc'])) $payload['ts_utc'] = gmdate('c');

    echo json_encode($payload);
    exit;
}

function api_error($httpCode, $code, $message, $context = array()) {
    api_send($httpCode, array(
        'ok' => false,
        'error' => array(
            'code' => $code,
            'message' => $message,
        ),
        'context' => $context,
    ));
}

set_error_handler(function($severity, $message, $file, $line) {
    // converte warning/notice em resposta JSON padronizada
    api_error(500, 'PHP_RUNTIME_WARNING', $message, array(
        'file' => $file,
        'line' => $line,
        'severity' => $severity,
    ));
});

set_exception_handler(function($ex) {
    api_error(500, 'PHP_EXCEPTION', $ex->getMessage(), array(
        'exception_class' => get_class($ex),
        'file' => $ex->getFile(),
        'line' => $ex->getLine(),
    ));
});

register_shutdown_function(function() {
    $err = error_get_last();
    if ($err && !$GLOBALS['__api_sent']) {
        api_error(500, 'PHP_FATAL', $err['message'], array(
            'file' => $err['file'],
            'line' => $err['line'],
            'type' => $err['type'],
        ));
    }
});

ob_start();

if(!empty($_POST['token']) && $_POST['token'] == $tokenAuthorized){

    if($_POST['action'] == "updateStatus"){
        $sqlUpdate = "UPDATE configuracao.tblogjava SET log_data = now() WHERE log_id = 1 AND log_status = 1";

        $resultEventoBd = null;
        $resultEventoBd = alterarRegistro($db,$sqlUpdate);

        $status['status'] = 200;
        $status['text'] = 'OK ' . $resultEventoBd;

//        echo json_encode($status);
        api_send(200, $status);
    }

    if($_POST['action'] == "sendWPData"){

        $requestId = isset($_POST['request_id']) ? trim($_POST['request_id']) : generateUUID();

        if (empty($_POST['jsonData'])) {
            api_error(400, 'MISSING_JSONDATA', 'Campo jsonData ausente ou vazio', array('request_id' => $requestId));
        }

        // valida JSON antes de seguir
        $tmp = json_decode($_POST['jsonData'], true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            api_error(400, 'INVALID_JSON', json_last_error_msg(), array(
                'request_id' => $requestId,
                'snippet' => substr($_POST['jsonData'], 0, 1500),
            ));
        }

        $type = isset($_POST['type']) ? $_POST['type'] : '';
        $jsonRetorno = InsertBanco($db, $type, $_POST['jsonData'], $requestId); // vamos ajustar assinatura

        // $jsonRetorno pode ser string JSON (compat) => decodifica para decidir ok/text
        $jrArr = json_decode($jsonRetorno, true);
        $outcome = isset($jrArr['Resultado']) ? $jrArr['Resultado'] : null;

        $ok = ($outcome === 'SUCCESS' || $outcome === 'DUPLICATE');

        $status = array(
            'ok' => $ok,
            'status' => 200,
            'text' => $ok ? 'OK' : 'ERROR',
//            'request_id' => $requestId,
            'type'        => $type,
            'jsonRetorno' => $jsonRetorno,
        );

//        echo json_encode($status);
        api_send(200, $status);
        exit;
    }

}
api_error(401, 'UNAUTHORIZED', 'Token invalido ou ausente');

function InsertBanco($db, $type, $jsonData, $requestId){

    $existente = '';
    $sqllinh_id = '';
    $EmailAddresses = '';
    $executaSql = True;  			//EXCECUTAR COMANDOS SQL
    $logGrava = False;				//GRAVAR LOGS DE SQL ARQUIVO TXT
    $printLogJson = True;
    $FileName = null;
    $DateRange = null;
    $linh_id = null;
    $repetido = array();

    $jsonRetorno = array();
    //    $jsonRetorno['request_id'] = $requestId;
    $jsonRetorno['Resultado'] = 'ERROR'; // default pessimista
    $jsonRetorno['Errors'] = array();
    $jsonRetorno['Aviso'] = array();
    $jsonRetorno['Metrics'] = array(
        'insert_ok' => 0,
        'insert_fail' => 0,
    );
    $jsonRetorno['GravaBanco'] = false;

    unset($jsonRetorno['request_id']);

    $addError = function($code, $message, $ctx = array()) use (&$jsonRetorno) {
        $jsonRetorno['Errors'][] = array(
            'code' => $code,
            'message' => $message,
            'context' => $ctx,
        );
    };

    $addWarning = function($code, $message, $ctx = array()) use (&$jsonRetorno) {
        $jsonRetorno['Aviso'][] = array(
            'code' => $code,
            'message' => $message,
            'context' => $ctx,
        );
    };

    if (!isset($jsonData) || $jsonData === '') {
        $addError('MISSING_JSONDATA', 'jsonData vazio dentro do InsertBanco');
        return json_encode($jsonRetorno);
    }

    if(isset($jsonData)){

        //CONVERTENDO EM JSON
        $json = json_decode($jsonData);
        if ($json === null && json_last_error() !== JSON_ERROR_NONE) {
            $addError('INVALID_JSON', json_last_error_msg(), array('snippet' => substr($jsonData, 0, 1500)));
            return json_encode($jsonRetorno);
        }

        //EXTRAÇÃO DO CABEÇALHO DO PACOTE - (evita notices)
        $FileName = isset($json->FileName) ? trim(pg_escape_string($json->FileName)) : null;

        // Mantém apenas números do nome do arquivo (remove PRTT, DADOS, .zip, etc)
        if (preg_match('/(\d{10,})/', $FileName, $matches)) { $FileNameId = $matches[1]; } else { $FileNameId = null; }

        $FileNameFinal = $FileName;
        if (!empty($FileNameFinal)) {
            $info = pathinfo($FileNameFinal);
            $base = isset($info['filename']) ? $info['filename'] : $FileNameFinal; // sem extensão
            $ext  = isset($info['extension']) && $info['extension'] !== '' ? ('.' . $info['extension']) : '';
            $posUnd = strrpos($base, '_');
            if ($posUnd !== false) {
                $base = substr($base, 0, $posUnd);
            }
            $FileNameFinal = $base . $ext;
        }

        $Unidade = isset($json->Unidade) ? (int)trim(pg_escape_string($json->Unidade)) : null;
        $InternalTicketNumber = isset($json->InternalTicketNumber) ? trim(pg_escape_string($json->InternalTicketNumber)) : null;
        $AccountIdentifier = isset($json->AccountIdentifier) ? trim(pg_escape_string(preg_replace('/[^0-9]/','',$json->AccountIdentifier))) : null;
        $AccountType = isset($json->AccountType) ? trim(pg_escape_string($json->AccountType)) : null;
        $Generated = isset($json->Generated) ? trim(pg_escape_string($json->Generated)) : null;
        $DateRange = isset($json->DateRange) ? trim(pg_escape_string($json->DateRange)) : null;
        $Service = isset($json->Service) ? trim(pg_escape_string($json->Service)) : null;

        $jsonRetorno['TypePRTTouDADOS'] = trim(pg_escape_string($type));
        $jsonRetorno['FileName'] = trim(pg_escape_string($FileNameFinal));
        $jsonRetorno['AccountIdentifier'] = trim(pg_escape_string($AccountIdentifier));
        $jsonRetorno['Unidade'] = trim(pg_escape_string($Unidade));
        $jsonRetorno['UnidName'] = find_unidade($db, $Unidade);
        $jsonRetorno['InternalTicketNumber'] = trim(pg_escape_string($InternalTicketNumber));

        if (empty($Unidade) || empty($jsonRetorno['UnidName'])) {
            $jsonRetorno['Resultado'] = 'ERROR';
            $jsonRetorno['GravaBanco'] = false;
            $addError('UNIDADE_NOT_FOUND', 'Unidade inválida ou não encontrada', array(
                'Unidade' => $Unidade,
                'FileName' => $FileName,
    //                'request_id' => $requestId,
            ));
            return json_encode($jsonRetorno);
        }
        function gerarVariante9($numero) {
            $len = strlen($numero);
            if (substr($numero, 0, 2) === '55') {
                if ($len === 13 && $numero[4] === '9') {
                    return substr($numero, 0, 4) . substr($numero, 5);
                } elseif ($len === 12) {
                    return substr($numero, 0, 4) . '9' . substr($numero, 4);
                }
            } elseif ($len === 11 && $numero[2] === '9') {
                return substr($numero, 0, 2) . substr($numero, 3);
            } elseif ($len === 10) {
                return substr($numero, 0, 2) . '9' . substr($numero, 2);
            }
            return null;
        }

    //        IMPLEMENTANDO A LOGICA PARA VARIAS LINH_ID
        if(!empty($AccountIdentifier) && $AccountIdentifier != '' && $AccountIdentifier != ' ' && !empty($Unidade) && $Unidade > 0){
            $query = null;

            //ARQUIVOS DO TIPO DADOS
            if($type == "DADOS"){
                $queryArId = null;

                //TRATAMENTO, DO NUMERO, PARA O LEITOR DE CONTA WHATSAPP
                $sqlTratamento = "SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL;";
                $queryTratamento = selectpadraoumalinha($db,$sqlTratamento);
                if(!empty($queryTratamento['conta_id']) && $queryTratamento['conta_id'] > 0){
                    $conta_id = trim(pg_escape_string(preg_replace('/[^0-9]/','',$queryTratamento['conta_id'])));
                    $apli_id = trim($queryTratamento['apli_id']);
                    $linh_id = trim($queryTratamento['linh_id']);

                    if (empty($linh_id) || empty($apli_id)) {
                        $jsonRetorno['Resultado'] = 'ERROR';
                        $jsonRetorno['GravaBanco'] = false;
                        $addError('TRATAMENTO_ZAP', 'Registro inválido (sem linh_id/apli_id).', array(
                            'raw_queryTratamento' => $queryTratamento,
                        ));
                        return json_encode($jsonRetorno);
                    }

                    $sqlUpdate = "UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '".$conta_id."' WHERE conta_zap IS NULL AND apli_id = ".$apli_id." AND linh_id = ".$linh_id.";";
                    if ($executaSql){
                        $resultEventoBd = null;
                        $resultEventoBd = alterarRegistro($db,$sqlUpdate);

                        if($printLogJson){
                            $jsonRetorno['1'] = 'OK ' . $resultEventoBd;
                        }
                    }

                    if($logGrava){
                        gravalog($FileName, "1");
                        gravalog($FileName, $sqlUpdate);
                    }
                }

                //Tratamento para DataRange - pega a data final do DataRange para utilizar no select($sqllinh_id)
                preg_match('/to\s(.+?)\sUTC/', $DateRange, $m);
                $dataFinal = isset($m[1]) ? date('Y-m-d', strtotime($m[1])) : null;

                // Utiliza DataRange para localizar o linh_id correto referente ao pacote
                $sqllinh_id = "SELECT lf.linh_id FROM interceptacao.tbobje_intercepta oi JOIN interceptacao.tboficio ofi ON ofi.ofic_id = oi.ofic_id JOIN linha_imei.tbaplicativo_linhafone lf
                                    ON lf.linh_id = oi.linh_id WHERE lf.apli_id = 1 AND lf.status = 'A' AND oi.opra_id = 28 AND oi.unid_id = $Unidade
                                    AND lf.conta_zap = '$AccountIdentifier' AND ofi.ofic_data = '$dataFinal' ORDER BY ofi.ofic_data;";
                $linhas = selectpadrao($db, $sqllinh_id);
                //FALLBACK: se não encontrou utilizando o DataRange tenta sem o DataRange e mais atual utilizando o dtcadastro
                if (empty($linhas)) {
                    $sqllinh_id = "SELECT lf.linh_id FROM interceptacao.tbobje_intercepta oi LEFT JOIN interceptacao.tboficio ofi ON ofi.ofic_id = oi.ofic_id JOIN linha_imei.tbaplicativo_linhafone lf
                                        ON lf.linh_id = oi.linh_id WHERE lf.apli_id = 1 AND lf.status = 'A' AND oi.opra_id = 28 AND oi.unid_id = $Unidade
                                        AND lf.conta_zap = '$AccountIdentifier' ORDER BY lf.dtcadastro DESC LIMIT 1;";
                    $linhas = selectpadrao($db, $sqllinh_id);
                    if ($printLogJson) {
                        $jsonRetorno['LINH_ID_FALLBACK'] = 'Executando sqllinh_id.';
                    }
                }

                // FALLBACK variante: tenta com/sem o dígito 9 após o DDD (padrão brasileiro)
                if (empty($linhas)) {
                    $AccountIdentifierVariante = gerarVariante9($AccountIdentifier);
                    if (!empty($AccountIdentifierVariante)) {
                        $sqllinh_id = "SELECT lf.linh_id FROM interceptacao.tbobje_intercepta oi LEFT JOIN interceptacao.tboficio ofi ON ofi.ofic_id = oi.ofic_id JOIN linha_imei.tbaplicativo_linhafone lf
                                            ON lf.linh_id = oi.linh_id WHERE lf.apli_id = 1 AND lf.status = 'A' AND oi.opra_id = 28 AND oi.unid_id = $Unidade
                                            AND lf.conta_zap = '$AccountIdentifierVariante' ORDER BY lf.dtcadastro DESC LIMIT 1;";
                        $linhas = selectpadrao($db, $sqllinh_id);

                        if (!empty($linhas) && is_array($linhas)) {
                            foreach ($linhas as $linhaVariante) {
                                $linh_id_upd = (int)$linhaVariante['linh_id'];
                                $sqlUpdateContaZap = "UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '$AccountIdentifier' WHERE linh_id = $linh_id_upd AND apli_id = 1;";
                                if ($executaSql) {
                                    alterarRegistro($db, $sqlUpdateContaZap);
                                }
                            }
                            if ($printLogJson) {
                                $jsonRetorno['VARIANTE-9'] = "Update em conta_zap: $AccountIdentifierVariante -> $AccountIdentifier";
                            }
                        }
                    }
                }

                if($logGrava){
                    gravalog($FileName, "2");
                    gravalog($FileName, $sqllinh_id);
                }

                if (empty($linhas) || !is_array($linhas)) {
                    $jsonRetorno['Resultado'] = 'ERROR';
                    $jsonRetorno['GravaBanco'] = false;
                    $addError('LINHA_NOT_FOUND', 'Linha não encontrada para Unid_ID='.$Unidade, [
                        'AccountIdentifier' => $AccountIdentifier,
                        'FileName' => $FileName,
                        'Unidade' => $Unidade
                    ]);
                    return json_encode($jsonRetorno);
                }

                if($printLogJson){
                    $jsonRetorno['2'] = 'OK - Qtd. Linhas: ' . count($linhas);
                }

                if (count($linhas) > 1) {
                    // ── REDISTRIBUIÇÃO ────────────────────────────────────────────────────────
                    // Busca o MELHOR candidato para redistribuição:
                    // linh_id que já tem arquivo para este account+daterange, mas de pacote diferente
                    // ORDER BY ar_dtcadastro ASC → garante que sempre pega a linha mais antiga (não a mesma)
                    $linh_ids_str = implode(',', array_column($linhas, 'linh_id'));

                    $sqlCandidato = "SELECT ar_id, linh_id
                                         FROM leitores.tb_whatszap_arquivo
                                         WHERE ar_tipo = 1
                                           AND linh_id IN ($linh_ids_str)
                                           AND telefone = '$AccountIdentifier'
                                           AND ar_dtgerado = '$DateRange'
                                           AND ar_arquivo NOT LIKE '$FileNameId%'
                                         ORDER BY ar_dtcadastro ASC
                                         LIMIT 1";
                    $candidato = selectpadraoumalinha($db, $sqlCandidato);

                    $linh_id_redistribuir = null;

                    if (is_array($candidato) && !empty($candidato['ar_id'])) {
                        $ar_id_redistribuir   = (int)$candidato['ar_id'];
                        $linh_id_redistribuir = (int)$candidato['linh_id'];

                        // Apaga tabelas filhas referenciando o ar_id antigo
                        foreach ([
                                     'leitores.tb_whatszap_iptime',
                                     'leitores.tb_whatszap_conexaoinfo',
                                     'leitores.tb_whatszap_weinfo',
                                     'leitores.tb_whatszap_grupoinfo',
                                     'leitores.tb_whatszap_agenda',
                                     'leitores.tb_whatszap_smallbusinessinfo',
                                     'leitores.tb_whatszap_deviceinfo',
                                 ] as $_tbl) {
                            if ($executaSql) {
                                inserirRegistro($db, "DELETE FROM $_tbl WHERE ar_id = $ar_id_redistribuir");
                            }
                            if ($logGrava) { gravalog($FileName, "REDISTRIBUICAO DEL $_tbl ar_id=$ar_id_redistribuir"); }
                        }

                        // Limpa referência no ticket (não apaga o ticket, só desvincula o ar_id)
                        if ($executaSql) {
                            inserirRegistro($db, "UPDATE leitores.tb_whatszap_ticketnumber SET ar_id = NULL WHERE ar_id = $ar_id_redistribuir");
                        }

                        // Apaga o arquivo principal
                        if ($executaSql) {
                            inserirRegistro($db, "DELETE FROM leitores.tb_whatszap_arquivo WHERE ar_id = $ar_id_redistribuir");
                        }

                        if ($printLogJson) {
                            $jsonRetorno['Redistribuicao'] = [
                                'linh_id'           => $linh_id_redistribuir,
                                'ar_id_apagado'     => $ar_id_redistribuir,
                            ];
                        }
                    }
                    // ── FIM REDISTRIBUIÇÃO ────────────────────────────────────────────────────
                } else {
                    $linh_id_redistribuir = null;
                }

                $linhasProcessadas = 0;
                $linhasDuplicadas  = 0;
                $db->con->beginTransaction();
                foreach ($linhas as $linha) {
                    $linh_id = $linha['linh_id'];

                    //BUSCA alvo_id E oper_id
                    $sqlRel = "SELECT lf.alvo_id, ta.oper_id FROM linha_imei.tblinhafone lf JOIN sistema.tbalvo ta ON ta.alvo_id = lf.alvo_id WHERE lf.linh_id = $linh_id LIMIT 1";
                    $rel = selectpadraoumalinha($db, $sqlRel);

                    if (empty($rel['alvo_id']) || empty($rel['oper_id'])) {
                        $jsonRetorno['Resultado'] = 'ERROR';
                        $jsonRetorno['GravaBanco'] = false;
                        $addError('RELATION_NOT_FOUND', 'Não foi possível determinar alvo_id/oper_id.', [
                            'linh_id' => $linh_id
                        ]);
                        $db->con->rollBack();
                        return json_encode($jsonRetorno);
                    } else {
                        $alvo_id = $rel['alvo_id'];
                        $oper_id = $rel['oper_id'];
                    }

                    // Se DADOS veio sem TicketNumber
                    if (empty($InternalTicketNumber)) {
                        $addError('TICKET_REQUIRED', 'DADOS sem ticketnumber');
                        $db->con->rollBack();
                        return json_encode($jsonRetorno);
                    }

                    //Verificar se o arquivo ja foi processado anteriormente
                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = $linh_id AND telefone = '$AccountIdentifier' AND ar_dtgerado = '$DateRange' AND ar_arquivo = '$FileNameFinal'";
                    $repetido = selectpadraoumalinha($db, $sqlexistente);

                    if (!empty($repetido['ar_id'])) {
                        $linhasDuplicadas++;
                        continue; // pula essa linha específica
                    }

                    // 2) Se esta linh_id não é a candidata de redistribuição,
                    //    mas possui OUTRO arquivo (de pacote anterior), pula sem tocar
                    if ($linh_id_redistribuir !== null && (int)$linh_id !== $linh_id_redistribuir) {
                        $sqlOutro = "SELECT ar_id FROM leitores.tb_whatszap_arquivo
                                     WHERE ar_tipo = 1
                                       AND linh_id = $linh_id
                                       AND telefone = '$AccountIdentifier'
                                       AND ar_dtgerado = '$DateRange'";
                        $outro = selectpadraoumalinha($db, $sqlOutro);
                        if (!empty($outro['ar_id'])) {
                            $linhasDuplicadas++;
                            continue; // outra linha, outro pacote → não toca
                        }
                    }
                    // A partir daqui: linh_id está livre (vazia ou foi redistribuída) → segue para inserção normal

                    // Verifica se já existe ticketnumber
                    $sqlTicket = "SELECT ticket_id FROM leitores.tb_whatszap_ticketnumber WHERE ticketnumber = '$InternalTicketNumber' AND linh_id = $linh_id LIMIT 1";
                    $ticket = selectpadraoumalinha($db, $sqlTicket);
                    if (!empty($ticket['ticket_id'])) {
                        $ticket_id = $ticket['ticket_id'];
                    } else {
                        // cria ticket
                        $sqlInsertTicket = "INSERT INTO leitores.tb_whatszap_ticketnumber (ticketnumber, linh_id, account_identifier, ar_id, alvo_id, oper_id) VALUES ( '$InternalTicketNumber', $linh_id, '$AccountIdentifier', NULL, $alvo_id, $oper_id ) RETURNING ticket_id";
                        $ticketInsert = inserirRegistroReturning($db, $sqlInsertTicket);

                        if (!is_array($ticketInsert) || empty($ticketInsert['ticket_id'])) {
                            $jsonRetorno['Metrics']['insert_fail']++;
                            $addError('TICKET_CREATE_FAIL', 'Falha ao criar ticket');
                            $db->con->rollBack();
                            return json_encode($jsonRetorno);
                        } else {
                            $jsonRetorno['Metrics']['insert_ok']++;
                            if ($printLogJson) {
                                $jsonRetorno['TicketId'] = 'OK - ' . $ticketInsert['ticket_id'];
                            }
                        }
                        $ticket_id = $ticketInsert['ticket_id'];
                    }

                    if (empty($repetido['ar_id'])) {
                        if (empty($DateRange) || empty($AccountIdentifier) || empty($linh_id)) {
                            $addWarning('SKIP_INSERT', 'Registro não gravado: ar_dtgerado, telefone ou linh_id ausentes.', [
                                'ar_dtgerado' => $DateRange, 'telefone' => $AccountIdentifier, 'linh_id' => $linh_id,
                            ]);
                            continue;
                        }
                        $fnBase = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_FILENAME));
                        $fnExt  = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_EXTENSION));
                        $sqlPlaceholder = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo IS NULL AND ar_status = 0 AND ar_dtgerado IS NULL AND telefone IS NULL AND linh_id IS NULL AND ar_arquivo LIKE '{$fnBase}%." . ($fnExt ?: '') . "' ORDER BY ar_dtcadastro DESC LIMIT 1";
                        $phRow = selectpadraoumalinha($db, $sqlPlaceholder);

                        if (!empty($phRow['ar_id'])) {
                            $sqlUpdatePh = "UPDATE leitores.tb_whatszap_arquivo SET telefone = '$AccountIdentifier', ar_dtgerado = '$DateRange', ar_arquivo = '$FileNameFinal', ar_tipo = 1, ar_status = 1, linh_id = $linh_id, ticket_id = $ticket_id, ar_dtcadastro = NOW() WHERE ar_id = " . (int)$phRow['ar_id'];
                            if ($executaSql) {
                                alterarRegistro($db, $sqlUpdatePh);
                                $queryArId = ['ar_id' => (int)$phRow['ar_id']];
                                $jsonRetorno['Metrics']['insert_ok']++;
                                $linhasProcessadas++;
                                if ($printLogJson) { $jsonRetorno['3'] = 'OK FILE BANCO (update) ' . $phRow['ar_id']; }
                            }
                        } else {
                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses, ticket_id) VALUES (" . $linh_id . ", '" . $AccountIdentifier . "', '" . $DateRange . "', NOW(), '" . $FileNameFinal . "', 1, 1, '" . $EmailAddresses . "'," . $ticket_id . ") RETURNING ar_id;";
                            if ($executaSql) {
                                $queryArId = inserirRegistroReturning($db, $sqlInsert);
                                if (!empty($ticket_id) && !empty($queryArId['ar_id'])) {
                                    $sqlUpdateTicket = "UPDATE leitores.tb_whatszap_ticketnumber SET ar_id = " . $queryArId['ar_id'] . " WHERE ticket_id = ". $ticket_id ." AND linh_id = ". $linh_id .";";
                                    alterarRegistro($db, $sqlUpdateTicket);
                                }
                                if (!$queryArId) {
                                    $jsonRetorno['Metrics']['insert_fail']++;
                                    $addError('DB_INSERT_FAIL', 'Falha - FILE BANCO', array(
                                        'sql' => $sqlInsert,
                                    ));
                                } else {
                                    $jsonRetorno['Metrics']['insert_ok']++;
                                    $linhasProcessadas++;
                                    if ($printLogJson) {
                                        $jsonRetorno['3'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                    }
                                }
                            }

                            if ($logGrava) {
                                gravalog($FileName, "3");
                                gravalog($FileName, $sqlInsert);
                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                            }
                        } // fim else INSERT DADOS

                        if (!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0) {

                            $ar_id = $queryArId['ar_id'];

                            if (isset($json->Dados->EmailAddresses)) {
                                $EmailAddresses = trim(pg_escape_string($json->Dados->EmailAddresses));

                                if (!empty($EmailAddresses)) {
                                    $sqlUpdate = "UPDATE leitores.tb_whatszap_arquivo SET ar_email_addresses = '" . $EmailAddresses . "' WHERE ar_id = " . $ar_id;

                                    $resultEventoBd = null;
                                    $resultEventoBd = alterarRegistro($db, $sqlUpdate);
                                }
                            }

                            if (isset($json->Dados->ipAddresses)) {
                                foreach ($json->Dados->ipAddresses as $registro) {
                                    if (isset($registro->IPAddress)) {
                                        $dadoIPAddress = trim(pg_escape_string($registro->IPAddress));
                                    } else {
                                        $dadoIPAddress = null;
                                    }
                                    if (isset($registro->Time)) {
                                        $dadoTime = trim(pg_escape_string(str_replace("UTC", "", $registro->Time)));
                                    } else {
                                        $dadoTime = null;
                                    }

                                    if (!empty($dadoIPAddress) && !empty($dadoTime)) {
                                        //GRAVANDO OS LOGS DE IP/TIME
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) VALUES ('" . $dadoIPAddress . "', '" . $dadoTime . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";

                                        if ($executaSql) {
                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if (!$resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha ipAddresses', array(
                                                    'sql' => $sqlInsert,
                                                ));
                                            } else {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['4'] = 'OK ' . $resultEventoBd;
                                                }
                                            }
                                        }
                                    }
                                    $existente = isset($existente) ? $existente : null;
                                    $sqlexistente = isset($sqlexistente) ? $sqlexistente : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "4");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                    }
                                }
                            }

                            if (isset($json->Dados->connectionInfo)) {
                                $connectionInfos = is_array($json->Dados->connectionInfo) ? $json->Dados->connectionInfo : [$json->Dados->connectionInfo];

                                foreach ($connectionInfos as $connectionInfo) {
                                    if (isset($connectionInfo->ServiceStart)) {
                                        $dadoServiceStart = trim(pg_escape_string($connectionInfo->ServiceStart));
                                    } else {
                                        $dadoServiceStart = null;
                                    }
                                    if (isset($connectionInfo->DeviceType)) {
                                        $dadoDeviceType = trim(pg_escape_string($connectionInfo->DeviceType));
                                    } else {
                                        $dadoDeviceType = null;
                                    }
                                    if (isset($connectionInfo->AppVersion)) {
                                        $dadoAppVersion = trim(pg_escape_string($connectionInfo->AppVersion));
                                    } else {
                                        $dadoAppVersion = null;
                                    }
                                    if (isset($connectionInfo->DeviceOSBuildNumber)) {
                                        $dadoDeviceOSBuildNumber = trim(pg_escape_string($connectionInfo->DeviceOSBuildNumber));
                                    } else {
                                        $dadoDeviceOSBuildNumber = null;
                                    }
                                    if (isset($connectionInfo->ConnectionState)) {
                                        $dadoConnectionState = trim(pg_escape_string($connectionInfo->ConnectionState));
                                    } else {
                                        $dadoConnectionState = null;
                                    }
                                    if (isset($connectionInfo->OnlineSince)) {
                                        $dadoOnlineSince = trim(pg_escape_string($connectionInfo->OnlineSince));
                                    } else {
                                        $dadoOnlineSince = null;
                                    }
                                    if (isset($connectionInfo->PushName)) {
                                        $dadoPushName = trim(pg_escape_string($connectionInfo->PushName));
                                    } else {
                                        $dadoPushName = null;
                                    }
                                    if (isset($connectionInfo->LastSeen)) {
                                        $dadoLastSeen = trim(pg_escape_string($connectionInfo->LastSeen));
                                    } else {
                                        $dadoLastSeen = null;
                                    }

                                    if (!empty($dadoServiceStart)) {
                                        //GRAVANDO CONEXÃO CONEXÃO INFO
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) VALUES ( '" . $dadoServiceStart . "', '" . $dadoDeviceType . "', '" . $dadoAppVersion . "', '" . $dadoDeviceOSBuildNumber . "', '" . $dadoConnectionState . "', '" . $dadoOnlineSince . "', '" . $dadoPushName . "', '" . $dadoLastSeen . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";

                                        if ($executaSql) {
                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if (!$resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha connectionInfo', array(
                                                    'sql' => $sqlInsert,
                                                ));
                                            } else {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['5'] = 'OK ' . $resultEventoBd;
                                                }
                                            }
                                        }
                                    }
                                    $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "5");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $resultEventoBd);
                                    }
                                }
                            }

                            if (isset($json->Dados->webInfo)) {
                                if (isset($json->Dados->webInfo->Version)) {
                                    $dadoVersion = trim(pg_escape_string($json->Dados->webInfo->Version));
                                } else {
                                    $dadoVersion = null;
                                }
                                if (isset($json->Dados->webInfo->Platform)) {
                                    $dadoPlatform = trim(pg_escape_string($json->Dados->webInfo->Platform));
                                } else {
                                    $dadoPlatform = null;
                                }
                                if (isset($json->Dados->webInfo->OnlineSince)) {
                                    $dadoOnlineSince = trim(pg_escape_string($json->Dados->webInfo->OnlineSince));
                                } else {
                                    $dadoOnlineSince = null;
                                }
                                if (isset($json->Dados->webInfo->InactiveSince)) {
                                    $dadoInactiveSince = trim(pg_escape_string($json->Dados->webInfo->InactiveSince));
                                } else {
                                    $dadoInactiveSince = null;
                                }
                                if (isset($json->Dados->webInfo->Availability)) {
                                    $Availability = trim(pg_escape_string($json->Dados->webInfo->Availability));
                                } else {
                                    $Availability = null;
                                }

                                if (!empty($dadoVersion)) {
                                    //GRAVANDO OS DADOS WEBINFO
                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) VALUES ('" . $dadoVersion . "', '" . $dadoPlatform . "', '" . $dadoOnlineSince . "', '" . $dadoInactiveSince . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";

                                    if ($executaSql) {
                                        $resultEventoBd = null;
                                        $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                        if (!$resultEventoBd) {
                                            $jsonRetorno['Metrics']['insert_fail']++;
                                            $addError('DB_INSERT_FAIL', 'Falha webInfo', array(
                                                'sql' => $sqlInsert,
                                            ));
                                        } else {
                                            $jsonRetorno['Metrics']['insert_ok']++;
                                        }

                                        if ($printLogJson) {
                                            $jsonRetorno['6'] = 'OK ' . $resultEventoBd;
                                        }
                                    }
                                }
                                $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                if ($logGrava) {
                                    gravalog($FileName, "6");
                                    gravalog($FileName, $sqlInsert);
                                    gravalog($FileName, $resultEventoBd);
                                }
                            }

                            // CORRIGIDO PARA PADRAO NOVO
                            if (isset($json->Dados->groupsInfo)) {
                                foreach ($json->Dados->groupsInfo->ownedGroups as $registro) {
                                    $dadoTipoGroup = 'Owned';
                                    if (isset($registro->Picture)) {
                                        $dadoPicture = trim(pg_escape_string($registro->Picture));
                                    } else {
                                        $dadoPicture = null;
                                    }
                                    if (isset($registro->LinkedMediaFile)) {
                                        $pathFile = trim(pg_escape_string($registro->LinkedMediaFile));
                                    } else {
                                        $pathFile = null;
                                    }
                                    if (isset($registro->Thumbnail)) {
                                        $dadoThumbnail = trim(pg_escape_string($registro->Thumbnail));
                                    } else {
                                        $dadoThumbnail = null;
                                    }
                                    if (isset($registro->ID)) {
                                        $dadoID = trim(pg_escape_string($registro->ID));
                                    } else {
                                        $dadoID = null;
                                    }
                                    if (isset($registro->Creation)) {
                                        $dadoCreation = trim(pg_escape_string($registro->Creation));
                                    } else {
                                        $dadoCreation = null;
                                    }
                                    if (isset($registro->Size)) {
                                        $dadoSize = trim(pg_escape_string($registro->Size));
                                    } else {
                                        $dadoSize = null;
                                    }
                                    if (isset($registro->Description)) {
                                        $dadoDescription = trim(pg_escape_string($registro->Description));
                                    } else {
                                        $dadoDescription = null;
                                    }
                                    if (isset($registro->Subject)) {
                                        $dadoSubject = trim(pg_escape_string($registro->Subject));
                                    } else {
                                        $dadoSubject = null;
                                    }

                                    if (!empty($dadoID)) {
                                        //GRAVANDO INFORMAÇÕES DO GRUPO OWNED
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('" . $dadoTipoGroup . "', '" . $pathFile . "', '" . $dadoThumbnail . "', '" . $dadoID . "', '" . $dadoCreation . "', '" . $dadoSize . "', '" . $dadoDescription . "', '" . $dadoSubject . "', '" . $AccountIdentifier . "', " . $ar_id . ", '" . $dadoPicture . "', " . $linh_id . ");";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if (!$resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha ownedGroups', array(
                                                    'sql' => $sqlInsert,
                                                ));
                                            } else {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                            }

                                            if ($printLogJson) {
                                                $jsonRetorno['7'] = 'OK ' . $resultEventoBd;
                                            }
                                        }
                                    }
                                    $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "7");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $resultEventoBd);
                                    }
                                }

                                foreach ($json->Dados->groupsInfo->ParticipatingGroups as $registro) {
                                    $dadoTipoGroup = 'Participating';
                                    if (isset($registro->Picture)) {
                                        $dadoPicture = trim(pg_escape_string($registro->Picture));
                                    } else {
                                        $dadoPicture = null;
                                    }
                                    if (isset($registro->LinkedMediaFile)) {
                                        $pathFile = trim(pg_escape_string($registro->LinkedMediaFile));
                                    } else {
                                        $pathFile = null;
                                    }
                                    if (isset($registro->Thumbnail)) {
                                        $dadoThumbnail = trim(pg_escape_string($registro->Thumbnail));
                                    } else {
                                        $dadoThumbnail = null;
                                    }
                                    if (isset($registro->ID)) {
                                        $dadoID = trim(pg_escape_string($registro->ID));
                                    } else {
                                        $dadoID = null;
                                    }
                                    if (isset($registro->Creation)) {
                                        $dadoCreation = trim(pg_escape_string($registro->Creation));
                                    } else {
                                        $dadoCreation = null;
                                    }
                                    if (isset($registro->Size)) {
                                        $dadoSize = trim(pg_escape_string($registro->Size));
                                    } else {
                                        $dadoSize = null;
                                    }
                                    if (isset($registro->Description)) {
                                        $dadoDescription = trim(pg_escape_string($registro->Description));
                                    } else {
                                        $dadoDescription = null;
                                    }
                                    if (isset($registro->Subject)) {
                                        $dadoSubject = trim(pg_escape_string($registro->Subject));
                                    } else {
                                        $dadoSubject = null;
                                    }

                                    if (!empty($dadoID)) {
                                        //GRAVANDO INFORMAÇÕES DO GRUPO PARTICIPATING
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('" . $dadoTipoGroup . "', '" . $pathFile . "', '" . $dadoThumbnail . "', '" . $dadoID . "', '" . $dadoCreation . "', '" . $dadoSize . "', '" . $dadoDescription . "', '" . $dadoSubject . "', '" . $AccountIdentifier . "', " . $ar_id . ", '" . $dadoPicture . "', " . $linh_id . ");";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if (!$resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha ParticipatingGroups', array(
                                                    'sql' => $sqlInsert,
                                                ));
                                            } else {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                            }

                                            if ($printLogJson) {
                                                $jsonRetorno['8'] = 'OK ' . $resultEventoBd;
                                            }
                                        }
                                    }
                                    $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "8");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $resultEventoBd);
                                    }
                                }
                            }

                            // CORRIGIDO PARA PADRAO NOVO
                            if (isset($json->Dados->addressBookInfo)) {
                                if (isset($json->Dados->addressBookInfo[0]->Symmetriccontacts)) {
                                    foreach ($json->Dados->addressBookInfo[0]->Symmetriccontacts as $registro) {
                                        $dadosymmetricContacts = trim(pg_escape_string($registro));

                                        //GRAVANDO TELEFONES SINCRONA
                                        if (isset($dadosymmetricContacts) && !empty($dadosymmetricContacts)) {
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('" . $dadosymmetricContacts . "', 'S', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";

                                            if ($executaSql) {
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                if (!$resultEventoBd) {
                                                    $jsonRetorno['Metrics']['insert_fail']++;
                                                    $addError('DB_INSERT_FAIL', 'Falha Symmetriccontacts', array(
                                                        'sql' => $sqlInsert,
                                                    ));
                                                } else {
                                                    $jsonRetorno['Metrics']['insert_ok']++;
                                                }

                                                if ($printLogJson) {
                                                    $jsonRetorno['9'] = 'OK ' . $resultEventoBd;
                                                }
                                            }
                                            $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                            if ($logGrava) {
                                                gravalog($FileName, "9");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $resultEventoBd);
                                            }
                                        }
                                    }
                                }

                                if (isset($json->Dados->addressBookInfo[0]->Asymmetriccontacts)) {

                                    foreach ($json->Dados->addressBookInfo[0]->Asymmetriccontacts as $registro) {
                                        $dadoasymmetricContacts = trim(pg_escape_string($registro));

                                        //GRAVANDO TELEFONES ASINCRONA
                                        if (isset($dadoasymmetricContacts) && !empty($dadoasymmetricContacts)) {
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('" . $dadoasymmetricContacts . "', 'A', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";

                                            if ($executaSql) {
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db, $sqlInsert);
                                                if (!$resultEventoBd) {
                                                    $jsonRetorno['Metrics']['insert_fail']++;
                                                    $addError('DB_INSERT_FAIL', 'Falha Asymmetriccontacts', array(
                                                        'sql' => $sqlInsert,
                                                    ));
                                                } else {
                                                    $jsonRetorno['Metrics']['insert_ok']++;
                                                    if ($printLogJson) {
                                                        $jsonRetorno['10'] = 'OK ' . $resultEventoBd;
                                                    }
                                                }
                                            }
                                            $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                            if ($logGrava) {
                                                gravalog($FileName, "10");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $resultEventoBd);
                                            }
                                        }
                                    }
                                }
                            }

                            if (isset($json->Dados->smallmediumbusinessinfo)) {
                                $sml_name = isset($json->Dados->smallmediumbusinessinfo->Name)
                                    ? trim(pg_escape_string($json->Dados->smallmediumbusinessinfo->Name))
                                    : null;

                                $sml_email = isset($json->Dados->smallmediumbusinessinfo->Email)
                                    ? trim(pg_escape_string($json->Dados->smallmediumbusinessinfo->Email))
                                    : null;

                                $sml_address = isset($json->Dados->smallmediumbusinessinfo->Address)
                                    ? trim(pg_escape_string($json->Dados->smallmediumbusinessinfo->Address))
                                    : null;
                                if ($sml_address && preg_match('/^(of business\.|generated|sample)/i', $sml_address)) {
                                    $sml_address = null;
                                }

                                $sml_websites = isset($json->Dados->smallmediumbusinessinfo->Websites)
                                    ? trim(pg_escape_string($json->Dados->smallmediumbusinessinfo->Websites))
                                    : null;

                                // Inserir SOMENTE se ao menos 1 campo preenchido (igual Python)
                                if ($sml_name || $sml_email || $sml_address || $sml_websites) {
                                    $sqlInsert = "
                                        INSERT INTO leitores.tb_whatszap_smallbusinessinfo
                                        (ar_id, linh_id, sml_name, sml_email, sml_address, sml_websites)
                                        VALUES (
                                            " . (int)$ar_id . ",
                                            " . (int)$linh_id . ",
                                            " . ($sml_name ? "'$sml_name'" : 'NULL') . ",
                                            " . ($sml_email ? "'$sml_email'" : 'NULL') . ",
                                            " . ($sml_address ? "'$sml_address'" : 'NULL') . ",
                                            " . ($sml_websites ? "'$sml_websites'" : 'NULL') . "
                                        )";

                                    if ($executaSql) {
                                        $resultEventoBd = inserirRegistro($db, $sqlInsert);
                                        if ($resultEventoBd) {
                                            $jsonRetorno['Metrics']['insert_ok']++;
                                            if ($printLogJson) {
                                                $jsonRetorno['18'] = 'OK ' . $resultEventoBd;
                                            }
                                        } else {
                                            $jsonRetorno['Metrics']['insert_fail']++;
                                            $addError('DB_INSERT_FAIL', 'Falha smallmediumbusinessinfo', [
                                                'sql' => $sqlInsert
                                            ]);
                                        }
                                    }
                                    $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "18");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $resultEventoBd);
                                    }
                                }
                            }

                            if (isset($json->Dados->ncmecReportsInfo)) {
                                //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
                                if (isset($json->Dados->ncmecReportsInfo->NcmecReportsDefinition)) {
                                    $NcmecReportsDefinition = trim(pg_escape_string($json->Dados->ncmecReportsInfo->NcmecReportsDefinition));
                                } else {
                                    $NcmecReportsDefinition = null;
                                }

                                if (isset($json->Dados->ncmecReportsInfo->NCMECCyberTipNumbers)) {
                                    $NCMECCyberTipNumbers = trim(pg_escape_string($json->Dados->ncmecReportsInfo->NCMECCyberTipNumbers));
                                } else {
                                    $NCMECCyberTipNumbers = null;
                                }

                                if ($printLogJson) {
                                    $jsonRetorno['19'] = 'OK FALTA FAZER ' . $NCMECCyberTipNumbers;
                                }
                            }

                            if (isset($json->Dados->deviceinfo)) {
                                $deviceInfos = is_array($json->Dados->deviceinfo) ? $json->Dados->deviceinfo : [$json->Dados->deviceinfo];
                                foreach ($deviceInfos as $deviceinfo) {

                                    if (isset($deviceinfo->AppVersion)) {
                                        $AppVersion = trim(pg_escape_string($deviceinfo->AppVersion));
                                    } else {
                                        $AppVersion = null;
                                    }

                                    if (isset($deviceinfo->OSVersion)) {
                                        $OSVersion = trim(pg_escape_string($deviceinfo->OSVersion));
                                    } else {
                                        $OSVersion = null;
                                    }

                                    if (isset($deviceinfo->OSBuildNumber)) {
                                        $OSBuildNumber = trim(pg_escape_string($deviceinfo->OSBuildNumber));
                                    } else {
                                        $OSBuildNumber = null;
                                    }

                                    if (isset($deviceinfo->DeviceManufacturer)) {
                                        $DeviceManufacturer = trim(pg_escape_string($deviceinfo->DeviceManufacturer));
                                    } else {
                                        $DeviceManufacturer = null;
                                    }

                                    if (isset($deviceinfo->DeviceModel)) {
                                        $DeviceModel = trim(pg_escape_string($deviceinfo->DeviceModel));
                                    } else {
                                        $DeviceModel = null;
                                    }

                                    if (!empty($AppVersion)) {
                                        //GRAVANDO INFORMAÇÕES DO GRUPO PARTICIPATING
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_deviceinfo (dev_appversion, dev_osversion, dev_buildnumber, dev_manufacturer, dev_devicemodel, ar_id, linh_id, telefone) VALUES ('" . $AppVersion . "', '" . $OSVersion . "', '" . $OSBuildNumber . "', '" . $DeviceManufacturer . "', '" . $DeviceModel . "', " . $ar_id . ", " . $linh_id . ", '" . $AccountIdentifier . "');";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);
                                            if (!$resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha deviceinfo', array(
                                                    'sql' => $sqlInsert,
                                                ));
                                            } else {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['20'] = 'OK ' . $resultEventoBd;
                                                }
                                            }
                                        }
                                    }
                                    $resultEventoBd = isset($resultEventoBd) ? $resultEventoBd : null;
                                    if ($logGrava) {
                                        gravalog($FileName, "20");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $resultEventoBd);
                                    }
                                }
                            }
                        }
                    }
                }
                $db->con->commit();
                if ($linhasProcessadas > 0) {

                    $jsonRetorno['Resultado'] = 'SUCCESS';
                    $jsonRetorno['GravaBanco'] = true;

                } elseif ($linhasDuplicadas > 0 && $linhasProcessadas == 0) {

                    $jsonRetorno['Resultado'] = 'DUPLICATE';
                    $jsonRetorno['GravaBanco'] = true;

                    $addWarning('DUPLICATE_FILE', 'Arquivo já processado para todas as linhas elegíveis', [
                        'FileName' => $FileName
                    ]);
                //
                    $jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
                    $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
                    $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . " Arquivo Existente \n\n");
                    fclose($FileLog );

                } else {

                    $jsonRetorno['Resultado'] = 'ERROR';
                    $jsonRetorno['GravaBanco'] = false;

                }
                $FileLog = fopen("ArquivoProcessados.txt", "a");
                $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");
                fclose($FileLog );
            }

            //ARQUIVOS DO TIPO PRTT
            if($type == "PRTT"){

                $queryArId = null;

                // PRTT AGORA DEPENDE EXCLUSIVAMENTE DO TICKET
                if (empty($InternalTicketNumber)) {
                    $addError('TICKET_REQUIRED', 'PRTT deve informar ticketnumber');
                    return json_encode($jsonRetorno);
                }

                //Procura o ticketnumber informado anteriomente pelo DADOS
                $sqlTicket = "SELECT ticket_id, linh_id FROM leitores.tb_whatszap_ticketnumber WHERE ticketnumber = '$InternalTicketNumber' AND account_identifier = '$AccountIdentifier';";
                $tickets = selectpadrao($db, $sqlTicket);

                // Fallback para ticket number, compatibilidade com dados antigos (antes da tabela de tickets)
                if (!is_array($tickets) || count($tickets) === 0) {
                    $sqllinh_id = "SELECT lf.linh_id FROM interceptacao.tbobje_intercepta oi LEFT JOIN interceptacao.tboficio ofi ON ofi.ofic_id = oi.ofic_id
                                        JOIN linha_imei.tbaplicativo_linhafone lf ON lf.linh_id = oi.linh_id WHERE lf.apli_id = 1 AND lf.status = 'A' AND oi.opra_id = 28
                                            AND oi.unid_id = $Unidade AND lf.conta_zap = '$AccountIdentifier' ORDER BY lf.dtcadastro DESC LIMIT 1;";
                    $queryFallback = selectpadraoumalinha($db, $sqllinh_id);
                    if ($printLogJson) {
                        $jsonRetorno['PRTT_FALLBACK'] = 'TicketNumber referência não encontrado(S/Dados).';
                    }

                    if (empty($queryFallback['linh_id'])) {
                        $addError('LINHA_NOT_FOUND', 'Fallback não conseguiu localizar linh_id para Unid_ID='.$Unidade);
                        return json_encode($jsonRetorno);
                    }

                    // Normaliza para a mesma estrutura do loop
                    $tickets = [['ticket_id' => null, 'linh_id' => $queryFallback['linh_id']]];
                }

                // ── LOOP: processa o pacote PRTT para cada linha elegível ──────────────────
                foreach ($tickets as $_ticket) {

                    $linh_id = isset($_ticket['linh_id']) ? (int)$_ticket['linh_id'] : null;
                    $ticket_id = isset($_ticket['ticket_id']) ? $_ticket['ticket_id'] : null;

                    if (empty($linh_id)) {
                        $addError('TICKET_INVALID', 'Ticket sem linh_id, ignorado.', ['ticket' => $_ticket]);
                        continue;
                    }

                    $ticketValue = empty($ticket_id) ? "NULL" : (int)$ticket_id;

                    if ($printLogJson && isset($ticket_id)) {
                        //$jsonRetorno['Ticket_IDs'][] = $ticket_id;
                        $jsonRetorno['Ticket_IDs'] = isset($jsonRetorno['Ticket_IDs'])
                            ? $jsonRetorno['Ticket_IDs'] . ', ' . $ticket_id
                            : (string)$ticket_id;
                    }

                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND linh_id = $linh_id AND telefone = '$AccountIdentifier' AND ar_dtgerado = '$DateRange' AND ar_arquivo = '$FileNameFinal'";
                    $repetido = selectpadraoumalinha($db, $sqlexistente);

                    if (empty($repetido['ar_id'])) {
                        if (empty($DateRange) || empty($AccountIdentifier) || empty($linh_id)) {
                            $addWarning('SKIP_INSERT', 'Registro não gravado: ar_dtgerado, telefone ou linh_id ausentes.', [
                                'ar_dtgerado' => $DateRange, 'telefone' => $AccountIdentifier, 'linh_id' => $linh_id,
                            ]);
                            continue;
                        }
                        $fnBase = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_FILENAME));
                        $fnExt  = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_EXTENSION));
                        $sqlPlaceholder = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo IS NULL AND ar_status = 0 AND ar_dtgerado IS NULL AND telefone IS NULL AND linh_id IS NULL AND ar_arquivo LIKE '{$fnBase}%." . ($fnExt ?: '') . "' ORDER BY ar_dtcadastro DESC LIMIT 1";
                        $phRow = selectpadraoumalinha($db, $sqlPlaceholder);

                        if (!empty($phRow['ar_id'])) {
                            $sqlUpdatePh = "UPDATE leitores.tb_whatszap_arquivo SET telefone = '$AccountIdentifier', ar_dtgerado = '$DateRange', ar_arquivo = '$FileNameFinal', ar_tipo = 0, ar_status = 1, linh_id = $linh_id, ticket_id = $ticketValue, ar_dtcadastro = NOW() WHERE ar_id = " . (int)$phRow['ar_id'];
                            if ($executaSql) {
                                alterarRegistro($db, $sqlUpdatePh);
                                $queryArId = ['ar_id' => (int)$phRow['ar_id']];
                                $jsonRetorno['Metrics']['insert_ok']++;
                                if ($printLogJson) { $jsonRetorno['11'] = 'OK FILE BANCO (update) ' . $phRow['ar_id']; }
                            }
                        } else {
                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id, ticket_id) VALUES ('" . $AccountIdentifier . "', '" . $DateRange . "', NOW(), '" . $FileNameFinal . "', 0, 1, " . $linh_id . "," . $ticketValue . ") RETURNING ar_id;";

                            if ($executaSql) {
                                $queryArId = inserirRegistroReturning($db, $sqlInsert);

                                if ($queryArId) {
                                    $jsonRetorno['Metrics']['insert_ok']++;
                                    if ($printLogJson) {
                                        $jsonRetorno['11'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                    }
                                } else {
                                    $jsonRetorno['Metrics']['insert_fail']++;
                                    $addError('DB_INSERT_FAIL', 'Falha insert tb_whatszap_arquivo ', [
                                        'sql' => $sqlInsert
                                    ]);
                                    continue;
                                }
                            }

                            if ($logGrava) {
                                gravalog($FileName, "11");
                                gravalog($FileName, $sqlInsert);
                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                            }
                        } // fim else INSERT PRTT

                        if (!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0) {

                            $ar_id = $queryArId['ar_id'];

                            //PRTT LOG DE CHAMADAS
                            if (isset($json->Prtt->callLogs)) {
                                foreach ($json->Prtt->callLogs as $registro) {
                                    if (isset($registro->CallId)) {
                                        $prttcallID = trim(pg_escape_string($registro->CallId));
                                    } else {
                                        $prttcallID = null;
                                    }
                                    if (isset($registro->CallCreator)) {
                                        $prttcallCreator = trim(pg_escape_string($registro->CallCreator));
                                    } else {
                                        $prttcallCreator = null;
                                    }
                                    if (isset($registro->Events)) {
                                        foreach ($registro->Events as $subregistro) {
                                            if (isset($subregistro->Type)) {
                                                $prttEtype = trim(pg_escape_string($subregistro->Type));
                                            } else {
                                                $prttEtype = null;
                                            }
                                            if (isset($subregistro->Timestamp)) {
                                                $prttEtimestamp = trim(pg_escape_string(str_replace("UTC", "", $subregistro->Timestamp)));
                                            } else {
                                                $prttEtimestamp = null;
                                            }
                                            if (isset($subregistro->From)) {
                                                $prttEsolicitante = trim(pg_escape_string($subregistro->From));
                                            } else {
                                                $prttEsolicitante = null;
                                            }
                                            if (isset($subregistro->To)) {
                                                $prttEatendente = trim(pg_escape_string($subregistro->To));
                                            } else {
                                                $prttEatendente = null;
                                            }
                                            if (isset($subregistro->FromIp)) {
                                                $prttEsolIP = trim(pg_escape_string($subregistro->FromIp));
                                            } else {
                                                $prttEsolIP = null;
                                            }
                                            if (isset($subregistro->FromPort)) {
                                                $prttEsolPort = trim(pg_escape_string($subregistro->FromPort));
                                            } else {
                                                $prttEsolPort = 0;
                                            }
                                            if (isset($subregistro->MediaType)) {
                                                $prttEmediaType = trim(pg_escape_string($subregistro->MediaType));
                                            } else {
                                                $prttEmediaType = null;
                                            }
                                            if ($prttcallCreator == $AccountIdentifier) {
                                                $TipoDirecaoCall = "EFETUOU";
                                            } else {
                                                $TipoDirecaoCall = "RECEBEU";
                                            }
                                            if (isset($subregistro->PhoneNumber)) {
                                                $prttPhoneNumber = trim(pg_escape_string($subregistro->PhoneNumber));
                                            } else {
                                                $prttPhoneNumber = null;
                                            }
                                            $estadosEncontrados = array();
                                            if (!empty($subregistro->Participants) && is_array($subregistro->Participants)) {
                                                foreach ($subregistro->Participants as $eventParticipant) {
                                                    // limpa e escapa os três campos
                                                    $pphone = isset($eventParticipant->PhoneNumber) ? pg_escape_string(trim($eventParticipant->PhoneNumber)) : null;
                                                    $pstate = isset($eventParticipant->State) ? pg_escape_string(trim($eventParticipant->State)) : null;
                                                    $platform = isset($eventParticipant->Platform) ? pg_escape_string(trim($eventParticipant->Platform)) : null;

                                                    // monta o INSERT estendendo o mesmo log, adicionando state + platform
                                                    $sqlPart = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, call_state, call_platform, telefone, ar_id, linh_id, sentido) VALUES ('{$prttcallID}', '{$prttcallCreator}', '{$prttEtype}', '{$prttEtimestamp}', '{$prttEsolicitante}', '{$prttEatendente}', '{$prttEsolIP}', '{$prttEsolPort}', '{$prttEmediaType}', '{$pphone}', '{$pstate}', '{$platform}', '{$AccountIdentifier}', {$ar_id}, {$linh_id}, '{$TipoDirecaoCall}');";

                                                    if ($executaSql) {
                                                        $resultEventoBd = inserirRegistro($db, $sqlPart);

                                                        if ($resultEventoBd) {
                                                            $jsonRetorno['Metrics']['insert_ok']++;
                                                            if ($printLogJson) {
                                                                $jsonRetorno['16'] = 'OK';
                                                            }
                                                        } else {
                                                            $jsonRetorno['Metrics']['insert_fail']++;
                                                            $addError('DB_INSERT_FAIL', 'Falha callLogs', [
                                                                'sql' => $sqlInsert
                                                            ]);
                                                        }
                                                        if ($logGrava) {
                                                            gravalog($FileName, trim($sqlPart));
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Verifique se cada estado específico está presente na prttEtimestamp
                                                $estadosEspecificos = array("Stateinvited", "Statereceipt", "Stateconnected", "Stateoutgoing", "StateconnectedPlatformandro", "ParticipantsPhone");
                                                foreach ($estadosEspecificos as $estado) {
                                                    if (strpos($prttEtimestamp, $estado) !== false) {
                                                        $estadosEncontrados[] = $estado;
                                                    }
                                                }

                                                if (empty($estadosEncontrados)) {
                                                    //INSERT DE CHAMADAS TROCADAS EM ALVO/INTERLOCUTOR
                                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES ('" . $prttcallID . "', '" . $prttcallCreator . "', '" . $prttEtype . "', '" . $prttEtimestamp . "', '" . $prttEsolicitante . "', '" . $prttEatendente . "', '" . $prttEsolIP . "', '" . $prttEsolPort . "', '" . $prttEmediaType . "', '" . $prttPhoneNumber . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ", '" . $TipoDirecaoCall . "');";

                                                    if ($executaSql) {
                                                        $resultEventoBd = null;
                                                        $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                        if (!$resultEventoBd) {
                                                            $jsonRetorno['Metrics']['insert_fail']++;
                                                            $addError('DB_INSERT_FAIL', 'Falha callLogs', array(
                                                                'sql' => $sqlInsert,
                                                            ));
                                                        } else {
                                                            $jsonRetorno['Metrics']['insert_ok']++;
                                                            if ($printLogJson) {
                                                                $jsonRetorno['17'] = 'OK ' . $resultEventoBd;
                                                            }
                                                        }
                                                    }
                                                }

                                                if ($logGrava) {
                                                    gravalog($FileName, "17");
                                                    gravalog($FileName, $sqlInsert);
                                                    gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                                }
                                            }
                                        }
                                    } else {
                                        $jsonRetorno['ErroCall'] = 'Erro Events Call';
                                    }
                                }
                            }

                            // PROSPECTIVE LOGIN IPs
                            if (isset($json->Prtt->prospectiveloginips)) {
                                foreach ($json->Prtt->prospectiveloginips as $registro) {

                                    $prospecTimestamp = isset($registro->Timestamp)
                                        ? trim(pg_escape_string(str_replace("UTC", "", $registro->Timestamp)))
                                        : null;

                                    $prospecIpAddress = isset($registro->IpAddress)
                                        ? trim(pg_escape_string($registro->IpAddress))
                                        : null;

                                    $prospecPort = isset($registro->Port)
                                        ? trim(pg_escape_string($registro->Port))
                                        : null;

                                    if ($executaSql) {

                                        $sqlexistente = "SELECT prospec_id
                                             FROM leitores.tb_whatszap_prospectiveloginips
                                             WHERE prospec_timestamp = '" . $prospecTimestamp . "'
                                               AND prospec_ipaddress = '" . $prospecIpAddress . "'
                                               AND prospec_port      = '" . $prospecPort . "'
                                               AND telefone          = '" . $AccountIdentifier . "';";
                                        $existente = duplicidadesql($db, $sqlexistente);

                                        if (empty($existente)) {
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_prospectiveloginips
                                                      (prospec_timestamp, prospec_ipaddress, prospec_port, telefone, ar_id, linh_id)
                                                  VALUES (
                                                      '" . $prospecTimestamp . "',
                                                      '" . $prospecIpAddress . "',
                                                      '" . $prospecPort      . "',
                                                      '" . $AccountIdentifier . "',
                                                      "  . $ar_id   . ",
                                                      "  . $linh_id . "
                                                  );";
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if ($resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['22'] = 'OK';
                                                }
                                            } else {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha prospectiveloginips', [
                                                    'sql' => $sqlInsert
                                                ]);
                                            }
                                        }
                                    }

                                    if ($logGrava) {
                                        gravalog($FileName, "22");
                                        gravalog($FileName, isset($sqlInsert) ? $sqlInsert : '');
                                        gravalog($FileName, (isset($existente) ? $existente : '') . ' - ' . (isset($sqlexistente) ? $sqlexistente : ''));
                                    }
                                }
                            }

                            //PRTT DE MENSSAGENS
                            if (isset($json->Prtt->msgLogs)) {
                                foreach ($json->Prtt->msgLogs as $registro) {
                                    if (isset($registro->Timestamp)) {
                                        $prttTimestamp = trim(pg_escape_string(str_replace("UTC", "", $registro->Timestamp)));
                                    } else {
                                        $prttTimestamp = null;
                                    }
                                    if (isset($registro->MessageId)) {
                                        $prttMessageId = trim(pg_escape_string($registro->MessageId));
                                    } else {
                                        $prttMessageId = null;
                                    }
                                    if (isset($registro->Sender)) {
                                        $prttSender = trim(pg_escape_string($registro->Sender));
                                    } else {
                                        $prttSender = null;
                                    }
                                    if (isset($registro->Recipients)) {
                                        $prttRecipients = trim(pg_escape_string($registro->Recipients));
                                    } else {
                                        $prttRecipients = null;
                                    }
                                    if (isset($registro->GroupId)) {
                                        $prttGroupId = trim(pg_escape_string($registro->GroupId));
                                    } else {
                                        $prttGroupId = null;
                                    }
                                    if (isset($registro->SenderIp)) {
                                        $prttSenderIp = trim(pg_escape_string($registro->SenderIp));
                                    } else {
                                        $prttSenderIp = null;
                                    }
                                    if (isset($registro->SenderPort)) {
                                        $prttSenderPort = trim(pg_escape_string($registro->SenderPort));
                                    } else {
                                        $prttSenderPort = null;
                                    }
                                    if (isset($registro->SenderDevice)) {
                                        $prttSenderDevice = trim(pg_escape_string($registro->SenderDevice));
                                    } else {
                                        $prttSenderDevice = null;
                                    }
                                    if (isset($registro->Type)) {
                                        $prttType = trim(pg_escape_string($registro->Type));
                                    } else {
                                        $prttType = null;
                                    }
                                    if (isset($registro->MessageStyle)) {
                                        $prttMessageStyle = trim(pg_escape_string($registro->MessageStyle));
                                    } else {
                                        $prttMessageStyle = null;
                                    }
                                    if (isset($registro->MessageSize)) {
                                        $prttMessageSize = trim(pg_escape_string($registro->MessageSize));
                                    } else {
                                        $prttMessageSize = null;
                                    }
                                    if (empty($prttGroupId)) {
                                        //VERIFICAÇÃO PARA INSERIR AS TROCAS DE MENSAGENS INDIVIDUAL
                                        if ($prttSender == $AccountIdentifier) {
                                            $TipoDirecaoMsg = "Enviou";
                                            //$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttSender."', '".$prttRecipients."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id." WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."' AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."');";
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('" . $prttTimestamp . "', '" . $prttMessageId . "', '" . $TipoDirecaoMsg . "', '" . $prttSender . "', '" . $prttRecipients . "', '" . $prttSenderIp . "', " . $prttSenderPort . ", '" . $prttSenderDevice . "', " . $prttMessageSize . ", '" . $prttType . "', '" . $prttMessageStyle . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";
                                            if ($executaSql) {
                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '" . $prttMessageId . "' AND datahora = '" . $prttTimestamp . "' AND telefone = '" . $AccountIdentifier . "';";
                                                $existente = duplicidadesql($db, $sqlexistente);
                                                if (empty($existente)) {
                                                    $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                    if ($resultEventoBd) {
                                                        $jsonRetorno['Metrics']['insert_ok']++;
                                                        if ($printLogJson) {
                                                            $jsonRetorno['12'] = 'OK';
                                                        }
                                                    } else {
                                                        $jsonRetorno['Metrics']['insert_fail']++;
                                                        $addError('DB_INSERT_FAIL', 'Falha msgLogs', [
                                                            'sql' => $sqlInsert
                                                        ]);
                                                    }
                                                }
                                            }

                                            if ($logGrava) {
                                                gravalog($FileName, "12");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        } else {
                                            $TipoDirecaoMsg = "Recebeu";
                                            //$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id." WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."'  AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."');";
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('" . $prttTimestamp . "', '" . $prttMessageId . "', '" . $TipoDirecaoMsg . "', '" . $prttSender . "', '" . $prttRecipients . "', '" . $prttSenderIp . "', " . $prttSenderPort . ", '" . $prttSenderDevice . "', " . $prttMessageSize . ", '" . $prttType . "', '" . $prttMessageStyle . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";
                                            if ($executaSql) {
                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '" . $prttMessageId . "'  AND datahora = '" . $prttTimestamp . "' AND telefone = '" . $AccountIdentifier . "';";
                                                $existente = duplicidadesql($db, $sqlexistente);
                                                if (empty($existente)) {
                                                    $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                    if ($resultEventoBd) {
                                                        $jsonRetorno['Metrics']['insert_ok']++;
                                                        if ($printLogJson) {
                                                            $jsonRetorno['13'] = 'OK';
                                                        }
                                                    } else {
                                                        $jsonRetorno['Metrics']['insert_fail']++;
                                                        $addError('DB_INSERT_FAIL', 'Falha msgLogs', [
                                                            'sql' => $sqlInsert
                                                        ]);
                                                    }
                                                }
                                            }
                                            if ($logGrava) {
                                                gravalog($FileName, "13");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        }
                                    } else {

                                        //Implementado para inserir participantes de acordo com o PRTT
                                        $sqlGoupId = "SELECT json_participantes FROM leitores.tb_whatszap_grupoinfo tbo WHERE tbo.id_msg = :id";
                                        $query = selectUmaLinha($db->con, $sqlGoupId, [':id' => $prttGroupId]);
                                        if ($query) {
                                            $atuais = isset($query['json_participantes']) ? trim($query['json_participantes']) : '';// Pega os participantes atuais
                                            $atuaisArray = array_filter(array_map('trim', explode(',', $atuais)));// Converte para array, removendo espaços e vazios
                                            $novosArray = array_filter(array_map('trim', explode(',', $prttRecipients)));// Converte os novos participantes
                                            $mergeArray = array_unique(array_merge($atuaisArray, $novosArray));// Faz o merge e remove duplicados
                                            $final = implode(', ', $mergeArray);// Transforma de volta em string
                                            $sqlParticipantsGroups = "UPDATE leitores.tb_whatszap_grupoinfo SET json_participantes = :json WHERE id_msg = :id";
                                            $update = executarUpdate($db->con, $sqlParticipantsGroups, [':json' => $final, ':id' => $prttGroupId]);
                                            if ($update > 0 && !empty($printLogJson)) {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['GroupParticipantes'] = 'OK';
                                                }
                                            } else {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha json_participantes', [
                                                    'sql' => $sqlInsert
                                                ]);
                                            }
                                        }

                                        //VERIFICAÇÃO PARA INSERIR AS TROCAS DE MENSAGENS GROUP
                                        if ($prttSender == $AccountIdentifier) {
                                            $TipoDirecaoMsg = "Enviou";
                                            //$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttSender."', '".$prttRecipients."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id." WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."' AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."');";
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('" . $prttTimestamp . "', '" . $prttMessageId . "', '" . $TipoDirecaoMsg . "', '" . $prttSender . "', '" . $prttRecipients . "', '" . $prttGroupId . "', '" . $prttSenderIp . "', " . $prttSenderPort . ", '" . $prttSenderDevice . "', " . $prttMessageSize . ", '" . $prttType . "', '" . $prttMessageStyle . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";
                                            if ($executaSql) {
                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '" . $prttMessageId . "' AND datahora = '" . $prttTimestamp . "' AND telefone = '" . $AccountIdentifier . "';";
                                                $existente = duplicidadesql($db, $sqlexistente);
                                                if (empty($existente)) {
                                                    $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                    if ($resultEventoBd) {
                                                        $jsonRetorno['Metrics']['insert_ok']++;
                                                        if ($printLogJson) {
                                                            $jsonRetorno['14'] = 'OK';
                                                        }
                                                    } else {
                                                        $jsonRetorno['Metrics']['insert_fail']++;
                                                        $addError('DB_INSERT_FAIL', 'Falha msgLogs', [
                                                            'sql' => $sqlInsert
                                                        ]);
                                                    }
                                                }
                                            }

                                            if ($logGrava) {
                                                gravalog($FileName, "14");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        } else {
                                            $TipoDirecaoMsg = "Recebeu";
                                            //$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) SELECT '".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id." WHERE NOT EXISTS (SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."'  AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."');";
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('" . $prttTimestamp . "', '" . $prttMessageId . "', '" . $TipoDirecaoMsg . "', '" . $prttSender . "',  '" . $prttRecipients . "','" . $prttGroupId . "', '" . $prttSenderIp . "', " . $prttSenderPort . ", '" . $prttSenderDevice . "', " . $prttMessageSize . ", '" . $prttType . "', '" . $prttMessageStyle . "', '" . $AccountIdentifier . "', " . $ar_id . ", " . $linh_id . ");";
                                            if ($executaSql) {
                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '" . $prttMessageId . "'  AND datahora = '" . $prttTimestamp . "' AND telefone = '" . $AccountIdentifier . "';";
                                                $existente = duplicidadesql($db, $sqlexistente);
                                                if (empty($existente)) {
                                                    $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                                    if ($resultEventoBd) {
                                                        $jsonRetorno['Metrics']['insert_ok']++;
                                                        if ($printLogJson) {
                                                            $jsonRetorno['15'] = 'OK';
                                                        }
                                                    } else {
                                                        $jsonRetorno['Metrics']['insert_fail']++;
                                                        $addError('DB_INSERT_FAIL', 'Falha Index ZapContatos', [
                                                            'sql' => $sqlInsert
                                                        ]);
                                                    }
                                                }
                                            }
                                            if ($logGrava) {
                                                gravalog($FileName, "15");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        }
                                    }
                                }
                            }

                            if (count($jsonRetorno['Errors']) === 0) {
                                $jsonRetorno['Resultado'] = 'SUCCESS';
                                $jsonRetorno['GravaBanco'] = true;
                            } else {
                                $jsonRetorno['Resultado'] = 'ERROR';
                                $jsonRetorno['GravaBanco'] = false;
                            }

                            $FileLog = fopen("ArquivoProcessados.txt", "a");
                            $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");
                            fclose($FileLog);
                        }

                    } else {
                        $jsonRetorno['Resultado'] = 'DUPLICATE';
                        $jsonRetorno['GravaBanco'] = true;
                        $addWarning('DUPLICATE_FILE', 'Arquivo já processado (duplicado)', array(
                            'FileName' => $FileName,
                            //                            'type' => $type,
                            'linh_id' => $linh_id,
                            'ar_id' => $repetido['ar_id'],
                        ));

                        $jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
                        $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
                        $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . "\nArquivo Existente \n\n");
                        fclose($FileLog);
                        $jsonRetorno['GravaBanco'] = False;
                    }
                }
            }

            //ARQUIVO DO TIPO SEM TAG DADOS OU PRTT
            if(empty($type) || $type == ''){
                $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileNameFinal."' AND ar_dtgerado = '".$DateRange."';";
                $repetido = selectpadraoconta($db, $sqlexistente);

                if (empty($repetido['ar_id'])){
                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id) VALUES ('".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileNameFinal."', 0, 1, ".$linh_id.") RETURNING ar_id;";

                    if ($executaSql){
                        $queryArId = inserirRegistroReturning($db,$sqlInsert);

                        if($queryArId){
                            $jsonRetorno['Metrics']['insert_ok']++;
                            if($printLogJson){
                                $jsonRetorno['21'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                            }
                        } else {
                            $jsonRetorno['Metrics']['insert_fail']++;
                            $addError('DB_INSERT_FAIL', 'Falha ao inserir registro', [
                                'sql' => $sqlInsert
                            ]);
                        }
                    }

                    if($logGrava){
                        gravalog($FileName, "21");
                        gravalog($FileName, $sqlInsert);
                        gravalog($FileName, $existente . ' - ' . $sqlexistente);
                    }
                }

                if (count($jsonRetorno['Errors']) === 0) {
                    $jsonRetorno['Resultado'] = 'SUCCESS';
                    $jsonRetorno['GravaBanco'] = true;
                } else {
                    $jsonRetorno['Resultado'] = 'ERROR';
                    $jsonRetorno['GravaBanco'] = false;
                }

                $FileLog = fopen("ArquivoProcessados.txt", "a");
                $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");
                fclose($FileLog );
            }

            $jsonRetorno['MostraJsonPython'] = False;
            $jsonRetorno['RetornoPHP'] = True;
            $jsonRetorno['ExibirTotalPacotesFila'] = False;
            $jsonRetorno['DataHora'] = date('Y-m-d H:i:s');

            //ARQUIVOS DO TIPO DADOS
            if($type == "GDADOS"){

                $sqlGrupo = "SELECT tbobje_whatsappgrupos.grupo_id, tbobje_intercepta.linh_id, tbobje_intercepta.obje_id, g.ulid AS grupo_ulid
                               FROM interceptacao.tbobje_whatsappgrupos
                               INNER JOIN interceptacao.tbobje_intercepta
                                  ON tbobje_intercepta.obje_id = tbobje_whatsappgrupos.obje_id
                               INNER JOIN interceptacao.tboficio
                                  ON tboficio.ofic_id = tbobje_intercepta.ofic_id
                               LEFT JOIN whatsapp.tbgrupowhatsapp g
                                  ON g.grupo_id::text ILIKE '%' || tbobje_whatsappgrupos.grupo_id || '%'
                               WHERE tbobje_intercepta.opra_id = 28
                                 AND tbobje_intercepta.unid_id = ".$Unidade."
                                 AND tbobje_whatsappgrupos.grupo_id ILIKE '%{$AccountIdentifier}%'
                               ORDER BY tboficio.ofic_data DESC
                               LIMIT 1;";
                $queryGrupo= selectpadraoumalinha($db,$sqlGrupo);

                $grupo_ulid = !empty($queryGrupo['grupo_ulid']) ? $queryGrupo['grupo_ulid'] : null;
                $jsonRetorno['Grupo_ulid'] = $grupo_ulid;

                if(!empty($queryGrupo['linh_id']) && $queryGrupo['linh_id'] > 0){
                    $queryArId = null;

                    $linh_id = $queryGrupo['linh_id'];

                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 2 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."'";
                    $repetido = selectpadraoumalinha($db, $sqlexistente);

                    if (empty($repetido['ar_id'])){
                        if (empty($DateRange) || empty($AccountIdentifier) || empty($linh_id)) {
                            $addWarning('SKIP_INSERT', 'Registro não gravado: ar_dtgerado, telefone ou linh_id ausentes.', [
                                'ar_dtgerado' => $DateRange, 'telefone' => $AccountIdentifier, 'linh_id' => $linh_id,
                            ]);
                        } else {
                        $fnBase = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_FILENAME));
                        $fnExt  = pg_escape_string(pathinfo($FileNameFinal, PATHINFO_EXTENSION));
                        $sqlPlaceholder = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo IS NULL AND ar_status = 0 AND ar_dtgerado IS NULL AND telefone IS NULL AND linh_id IS NULL AND ar_arquivo LIKE '{$fnBase}%." . ($fnExt ?: '') . "' ORDER BY ar_dtcadastro DESC LIMIT 1";
                        $phRow = selectpadraoumalinha($db, $sqlPlaceholder);

                        if (!empty($phRow['ar_id'])) {
                            $sqlUpdatePh = "UPDATE leitores.tb_whatszap_arquivo SET telefone = '$AccountIdentifier', ar_dtgerado = '$DateRange', ar_arquivo = '$FileNameFinal', ar_tipo = 2, ar_status = 1, linh_id = $linh_id, ar_dtcadastro = NOW() WHERE ar_id = " . (int)$phRow['ar_id'];
                            if ($executaSql) {
                                alterarRegistro($db, $sqlUpdatePh);
                                $queryArId = ['ar_id' => (int)$phRow['ar_id']];
                                $jsonRetorno['Metrics']['insert_ok']++;
                                if ($printLogJson) { $jsonRetorno['1G'] = 'OK FILE BANCO (update) ' . $phRow['ar_id']; }
                            }
                        } else {
                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES (".$linh_id.", '".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileNameFinal."', 2, 1) RETURNING ar_id;";

                            if ($executaSql){
                                $queryArId = inserirRegistroReturning($db,$sqlInsert);

                                if($queryArId){
                                    $jsonRetorno['Metrics']['insert_ok']++;
                                    if($printLogJson){
                                        $jsonRetorno['1G'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                    }
                                } else {
                                    $jsonRetorno['Metrics']['insert_fail']++;
                                    $addError('DB_INSERT_FAIL', 'Falha arquivo GDADOS', [
                                        'sql' => $sqlInsert
                                    ]);
                                }
                            }

                            if($logGrava){
                                gravalog($FileName, "1G");
                                gravalog($FileName, $sqlInsert);
                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                            }
                        } // fim else INSERT GDADOS
                        } // fim else guard GDADOS

                        if(!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0){
                            $ar_id = $queryArId['ar_id'];

                            $sqlIdentificador = "SELECT tbmembros_whats.identificador
                                                FROM whatsapp.tbmembros_whats, whatsapp.tbgrupowhatsapp, linha_imei.tbaplicativo_linhafone, linha_imei.tblinhafone
                                                WHERE  tbmembros_whats.grupo_id = tbgrupowhatsapp.grupo_id AND tbmembros_whats.identificador = tbaplicativo_linhafone.identificador
                                                  AND tblinhafone.linh_id = tbaplicativo_linhafone.linh_id AND tblinhafone.unid_id = ".$Unidade."
                                                  AND tbmembros_whats.grupo_id ILIKE '%".trim($AccountIdentifier)."%'";
                            $queryIdentificador = selectpadraoumalinha($db, $sqlIdentificador);

                            if (!empty($queryIdentificador['identificador'])) {
                                $identificador = $queryIdentificador['identificador'];
                                if (isset($json->GDados->groupsInfo)) {
                                    foreach ($json->GDados->groupsInfo->GroupParticipants as $registro) {
                                        $grupo_ulid_sql = $grupo_ulid ? "'" . $grupo_ulid . "'" : "NULL";
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_ulid, grupo_participante, grupo_adm, grupo_status, identificador)
                                                    VALUES ('" . trim($AccountIdentifier) . "', " . $grupo_ulid_sql . ", '" . somenteNumeros($registro) . "', 'N', 'A', " . $identificador . ");";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if ($resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['2G'] = 'OK ' . $resultEventoBd;
                                                }
                                            } else {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha GroupParticipants', [
                                                    'sql' => $sqlInsert
                                                ]);
                                            }
                                            if ($logGrava) {
                                                gravalog($FileName, "2G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        }
                                    }

                                    foreach ($json->GDados->groupsInfo->GroupAdministrators as $registro) {
                                        $grupo_ulid_sql = $grupo_ulid ? "'" . $grupo_ulid . "'" : "NULL";
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_ulid, grupo_participante, grupo_adm, grupo_status, identificador)
                                                    VALUES ('" . trim($AccountIdentifier) . "', " . $grupo_ulid_sql . ", '" . somenteNumeros($registro) . "', 'S', 'A', " . $identificador . ");";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if ($resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['3G'] = 'OK ' . $resultEventoBd;
                                                }
                                            } else {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha GroupAdministrators', [
                                                    'sql' => $sqlInsert
                                                ]);
                                            }
                                            if ($logGrava) {
                                                gravalog($FileName, "3G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        }
                                    }

                                    foreach ($json->GDados->groupsInfo->Participants as $registro) {
                                        $grupo_ulid_sql = $grupo_ulid ? "'" . $grupo_ulid . "'" : "NULL";
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_ulid, grupo_participante, grupo_adm, grupo_status, identificador)
                                                    VALUES ('" . trim($AccountIdentifier) . "', " . $grupo_ulid_sql . ", '" . somenteNumeros($registro) . "', 'N', 'A', " . $identificador . ");";

                                        if ($executaSql) {
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db, $sqlInsert);

                                            if ($resultEventoBd) {
                                                $jsonRetorno['Metrics']['insert_ok']++;
                                                if ($printLogJson) {
                                                    $jsonRetorno['4G'] = 'OK ' . $resultEventoBd;
                                                }
                                            } else {
                                                $jsonRetorno['Metrics']['insert_fail']++;
                                                $addError('DB_INSERT_FAIL', 'Falha GroupAdministrators', [
                                                    'sql' => $sqlInsert
                                                ]);
                                            }
                                            if ($logGrava) {
                                                gravalog($FileName, "4G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $existente . ' - ' . $sqlexistente);
                                            }
                                        }
                                    }
                                }

                                $FileLog = fopen("ArquivoProcessados.txt", "a");
                                $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");
                                fclose($FileLog );

                                if (count($jsonRetorno['Errors']) === 0) {
                                    $jsonRetorno['Resultado'] = 'SUCCESS';
                                    $jsonRetorno['GravaBanco'] = true;
                                } else {
                                    $jsonRetorno['Resultado'] = 'ERROR';
                                    $jsonRetorno['GravaBanco'] = false;
                                }
                                $jsonRetorno['MostraJsonPython'] = False;
                                $jsonRetorno['RetornoPHP'] = True;
                                $jsonRetorno['ExibirTotalPacotesFila'] = False;
                                $jsonRetorno['DataHora'] = date('Y-m-d H:i:s');
                            }else{
                                $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
                                $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \n" . $sqllinh_id ."\nLinha Nao Localizada \n\n");
                                fclose($FileLog );

                                $jsonRetorno['GravaBanco'] = False;
                                $jsonRetorno['AVISO_1G'] = 'Grupo ID Nao Localizado ' . $AccountIdentifier;
                            }
                        }
                    } else {
                        $jsonRetorno['Resultado'] = 'DUPLICATE';
                        $jsonRetorno['GravaBanco'] = true;
                        $addWarning('DUPLICATE_FILE', 'Arquivo já processado (duplicado)', array(
                            'FileName' => $FileName,
                            'type' => $type,
                            'linh_id' => $linh_id,
                            'ar_id' => $repetido['ar_id'],
                        ));

                        $jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
                        $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
                        $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . "\nArquivo Existente \n\n");
                        fclose($FileLog);
                        $jsonRetorno['GravaBanco'] = False;
                    }
                }else{
                    $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
                    $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \n" . $sqllinh_id ."\nLinha Nao Localizada \n\n");
                    fclose($FileLog );

                    $jsonRetorno['GravaBanco'] = False;
                    $jsonRetorno['AVISO_2G'] = 'Grupo ID Nao Localizado ' . $AccountIdentifier;
                }
            }
        }else{
            $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");
            $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] ."\nErro de Conta ou Unidade \n\n");
            fclose($FileLog );
            $jsonRetorno['GravaBanco'] = False;
            $jsonRetorno['AVISO_2'] = 'Erro Conta Zap ' . $AccountIdentifier;
        }
        if ($jsonRetorno['Resultado'] === 'ERROR' && count($jsonRetorno['Errors']) === 0) {
            $jsonRetorno['Resultado'] = 'NOT_LOCATED';
            $addError('LINE_NOT_FOUND', '7 - Linha não Localizada.', array(
                'FileName' => isset($FileName) ? $FileName : null,
                'Unidade'  => isset($Unidade) ? $Unidade : null,
                'AccountIdentifier' => isset($AccountIdentifier) ? $AccountIdentifier : null,
                'type' => $type,
    //                'request_id' => $requestId,
            ));
        }
        return json_encode($jsonRetorno);
    }
}

//AnaliseJson();

function AnaliseJson(){
    $json = json_decode(file_get_contents('jsonWhatsappPRTT.txt'));

    if(isset($json)){

        //EXTRAÇÃO DO CABEÇALHO DO PACOTE
        if(isset($json->FileName)){
            $FileName = trim(pg_escape_string($json->FileName));
        }
        if(isset($json->AccountIdentifier)){
            $AccountIdentifier = trim(pg_escape_string(preg_replace('/[^0-9]/','',$json->AccountIdentifier)));
        }
        if(isset($json->AccountType)){
            $AccountType = trim(pg_escape_string($json->AccountType));
        }
        if(isset($json->Generated)){
            $Generated = trim(pg_escape_string($json->Generated));
        }
        if(isset($json->DateRange)){
            $DateRange = trim(pg_escape_string($json->DateRange));
        }
        if(isset($json->EmailAddresses)){
            $EmailAddresses = trim(pg_escape_string($json->EmailAddresses));
        }

        //ARQUIVOS DO TIPO DADOS
        if(isset($json->Dados->ipAddresses)){
            foreach($json->Dados->ipAddresses as $registro){
                if(isset($registro->IPAddress)){
                    $dadoIPAddress = trim(pg_escape_string($registro->IPAddress));
                }else{
                    $dadoIPAddress = null;
                }
                if(isset($registro->Time)){
                    $dadoTime = trim(pg_escape_string(str_replace("UTC","",$registro->Time)));
                }else{
                    $dadoTime = null;
                }
            }
        }

        if(isset($json->Dados->connectionInfo)){
            if(isset($json->Dados->connectionInfo->ServiceStart)){
                $dadoServiceStart = trim(pg_escape_string($json->Dados->connectionInfo->ServiceStart));
            }else{
                $dadoServiceStart = null;
            }
            if(isset($json->Dados->connectionInfo->DeviceType)){
                $dadoDeviceType = trim(pg_escape_string($json->Dados->connectionInfo->DeviceType));
            }else{
                $dadoDeviceType = null;
            }
            if(isset($json->Dados->connectionInfo->AppVersion)){
                $dadoAppVersion = trim(pg_escape_string($json->Dados->connectionInfo->AppVersion));
            }else{
                $dadoAppVersion = null;
            }
            if(isset($json->Dados->connectionInfo->DeviceOSBuildNumber)){
                $dadoDeviceOSBuildNumber = trim(pg_escape_string($json->Dados->connectionInfo->DeviceOSBuildNumber));
            }else{
                $dadoDeviceOSBuildNumber = null;
            }
            if(isset($json->Dados->connectionInfo->ConnectionState)){
                $dadoConnectionState = trim(pg_escape_string($json->Dados->connectionInfo->ConnectionState));
            }else{
                $dadoConnectionState = null;
            }
            if(isset($json->Dados->connectionInfo->OnlineSince)){
                $dadoOnlineSince = trim(pg_escape_string($json->Dados->connectionInfo->OnlineSince));
            }else{
                $dadoOnlineSince = null;
            }
            if(isset($json->Dados->connectionInfo->PushName)){
                $dadoPushName = trim(pg_escape_string($json->Dados->connectionInfo->PushName));
            }else{
                $dadoOnlineSince = null;
            }
            if(isset($json->Dados->connectionInfo->LastSeen)){
                $dadoLastSeen = trim(pg_escape_string($json->Dados->connectionInfo->LastSeen));
            }else{
                $dadoLastSeen = null;
            }
        }

        if(isset($json->Dados->webInfo)){
            if(isset($json->Dados->webInfo->Version)){
                $dadoVersion = trim(pg_escape_string($json->Dados->webInfo->Version));
            }else{
                $dadoVersion = null;
            }
            if(isset($json->Dados->webInfo->Platform)){
                $dadoPlatform = trim(pg_escape_string($json->Dados->webInfo->Platform));
            }else{
                $dadoPlatform = null;
            }
            if(isset($json->Dados->webInfo->OnlineSince)){
                $dadoOnlineSince = trim(pg_escape_string($json->Dados->webInfo->OnlineSince));
            }else{
                $dadoOnlineSince = null;
            }
            if(isset($json->Dados->webInfo->InactiveSince)){
                $dadoInactiveSince = trim(pg_escape_string($json->Dados->webInfo->InactiveSince));
            }else{
                $dadoInactiveSince = null;
            }
        }

        if(isset($json->Dados->groupsInfo)){
            foreach($json->Dados->groupsInfo->ownedGroups as $registro){
                $dadoTipoGroup = 'Owned';
                $pathFile = null;
                if(isset($registro->Picture)){
                    $dadoPicture = trim(pg_escape_string($registro->Picture));
                }else{
                    $dadoPicture = null;
                }
                if(isset($registro->Thumbnail)){
                    $dadoThumbnail = trim(pg_escape_string($registro->Thumbnail));
                }else{
                    $dadoThumbnail = null;
                }
                if(isset($registro->ID)){
                    $dadoID = trim(pg_escape_string($registro->ID));
                }else{
                    $dadoID = null;
                }
                if(isset($registro->Creation)){
                    $dadoCreation = trim(pg_escape_string($registro->Creation));
                }else{
                    $dadoCreation = null;
                }
                if(isset($registro->Size)){
                    $dadoSize = trim(pg_escape_string($registro->Size));
                }else{
                    $dadoSize = null;
                }
                if(isset($registro->Description)){
                    $dadoDescription = trim(pg_escape_string($registro->Description));
                }else{
                    $dadoDescription =  null;
                }
                if(isset($registro->Subject)){
                    $dadoSubject = trim(pg_escape_string($registro->Subject));
                }else{
                    $dadoSubject =  null;
                }
            }

            foreach($json->Dados->groupsInfo->ParticipatingGroups as $registro){
                $dadoTipoGroup = 'Participating';
                $pathFile = null;
                if(isset($registro->Picture)){
                    $dadoPicture = trim(pg_escape_string($registro->Picture));
                }else{
                    $dadoPicture = null;
                }
                if(isset($registro->Picture)){
                    $dadoThumbnail = trim(pg_escape_string($registro->Thumbnail));
                }else{
                    $dadoThumbnail = null;
                }
                if(isset($registro->ID)){
                    $dadoID = trim(pg_escape_string($registro->ID));
                }else{
                    $dadoID = null;
                }
                if(isset($registro->Creation)){
                    $dadoCreation = trim(pg_escape_string($registro->Creation));
                }else{
                    $dadoCreation = null;
                }
                if(isset($registro->Size)){
                    $dadoSize = trim(pg_escape_string($registro->Size));
                }else{
                    $dadoSize = null;
                }
                if(isset($registro->Description)){
                    $dadoDescription = trim(pg_escape_string($registro->Description));
                }else{
                    $dadoDescription =  null;
                }
                if(isset($registro->Subject)){
                    $dadoSubject = trim(pg_escape_string($registro->Subject));
                }else{
                    $dadoSubject =  null;
                }
            }
        }

        if(isset($json->Dados->addressBookInfo)){

            foreach($json->Dados->addressBookInfo->symmetricContacts as $registro){
                $dadosymmetricContacts = trim(pg_escape_string($registro));
            }

            foreach($json->Dados->addressBookInfo->asymmetricContacts as $registro){
                $dadoasymmetricContacts = trim(pg_escape_string($registro));
            }
        }

        if(isset($json->Dados->smallMediumBusiness)){
            //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
            $dadosmallMediumBusiness = trim(pg_escape_string($json->Dados->smallMediumBusiness));
        }

        if(isset($json->Dados->ncmecReportsInfo)){
            //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
            $dadoncmecReports = trim(pg_escape_string($json->Dados->ncmecReportsInfo));
        }


        //ARQUIVOS DO TIPO PRTT

        if(isset($json->Prtt->msgLogs)){
            foreach($json->Prtt->msgLogs as $registro){
                if(isset($registro->Timestamp)){
                    $prttTimestamp = trim(pg_escape_string(str_replace("UTC","",$registro->Timestamp)));
                }else{
                    $prttTimestamp = null;
                }
                if(isset($registro->MessageId)){
                    $prttMessageId = trim(pg_escape_string($registro->MessageId));
                }else{
                    $prttMessageId = null;
                }
                if(isset($registro->Sender)){
                    $prttSender = trim(pg_escape_string($registro->Sender));
                }else{
                    $prttSender = null;
                }
                if(isset($registro->Recipients)){
                    $prttRecipients = trim(pg_escape_string($registro->Recipients));
                }else{
                    $prttRecipients = null;
                }
                if(isset($registro->GroupId)){
                    $prttGroupId = trim(pg_escape_string($registro->GroupId));
                }else{
                    $prttGroupId = null;
                }
                if(isset($registro->SenderIp)){
                    $prttSenderIp = trim(pg_escape_string($registro->SenderIp));
                }else{
                    $prttSenderIp = null;
                }
                if(isset($registro->SenderPort)){
                    $prttSenderPort = trim(pg_escape_string($registro->SenderPort));
                }else{
                    $prttSenderPort = 0;
                }
                if(isset($registro->SenderDevice)){
                    $prttSenderDevice = trim(pg_escape_string($registro->SenderDevice));
                }else{
                    $prttSenderDevice  = null;
                }
                if(isset($registro->Type)){
                    $prttType = trim(pg_escape_string($registro->Type));
                }else{
                    $prttType = null;
                }
                if(isset($registro->MessageStyle)){
                    $prttMessageStyle = trim(pg_escape_string($registro->MessageStyle));
                }else{
                    $prttMessageStyle = null;
                }
                if(isset($registro->MessageSize)){
                    $prttMessageSize = trim(pg_escape_string($registro->MessageSize));
                }else{
                    $prttMessageSize = null;
                }
            }
        }


        //PRTT LOG DE CHAMADAS
        if(isset($json->Prtt->callLogs)){
            foreach($json->Prtt->callLogs as $registro){
                if(isset($registro->callID)){
                    $prttcallID = trim(pg_escape_string($registro->callID));
                }else{
                    $prttcallID = null;
                }
                if(isset($registro->callCreator)){
                    $prttcallCreator = trim(pg_escape_string($registro->callCreator));
                }else{
                    $prttcallCreator = null;
                }
                if(isset($registro->Events)){

                    foreach($registro->Events as $subregistro){
                        if(isset($subregistro->type)){
                            $prttEtype = trim(pg_escape_string($subregistro->type));
                        }else{
                            $prttEtype = null;
                        }
                        if(isset($subregistro->timestamp)){
                            $prttEtimestamp = trim(pg_escape_string(str_replace("UTC","",$subregistro->timestamp)));
                        }else{
                            $prttEtimestamp = null;
                        }
                        if(isset($subregistro->solicitante)){
                            $prttEsolicitante = trim(pg_escape_string($subregistro->solicitante));
                        }else{
                            $prttEsolicitante = null;
                        }
                        if(isset($subregistro->atendente)){
                            $prttEatendente = trim(pg_escape_string($subregistro->atendente));
                        }else{
                            $prttEatendente = null;
                        }
                        if(isset($subregistro->solIP)){
                            $prttEsolIP = trim(pg_escape_string($subregistro->solIP));
                        }else{
                            $prttEsolIP = null;
                        }
                        if(isset($subregistro->solPort)){
                            $prttEsolPort = trim(pg_escape_string($subregistro->solPort));
                        }else{
                            $prttEsolPort = 0;
                        }
                        if(isset($subregistro->mediaType)){
                            $prttEmediaType = trim(pg_escape_string($subregistro->mediaType));
                        }else{
                            $prttEmediaType = null;
                        }
                        if(count($subregistro->Participants) > 0){
                            if (isset($subregistro->Participants)){
                                foreach($subregistro->Participants as $eventParticipant){
                                    if(isset($eventParticipant->PhoneNumber)){
                                        $callGruopParticipant = $eventParticipant->PhoneNumber;
                                    }else{
                                        $callGruopParticipant = null;
                                    }
                                }
                            }
                        }

                    }
                }
            }
        }

        /*if(isset($json->Prtt->fileContent)){
            $prttfileContent = trim(pg_escape_string($json->Prtt->fileContent));
        }*/
    }
}

function base64_to_convert($base64_string, $path) {
    $ifp = fopen($path, "wb" );
    fwrite($ifp, base64_decode($base64_string));
    fclose($ifp);
}

function find_unidade($db, $unid_id) {
    $sqlUnid = "SELECT * FROM sistema.tbunidade WHERE unid_id = " . $unid_id;
    $queryUnid = selectpadraoumalinha($db, $sqlUnid);
    return $queryUnid['unid_nome'];
}

function gravalog($filename,$content){
    $filename = str_replace(".zip", '', $filename);
    if (!file_exists('./Logs')) {
        mkdir('./Logs', 0777, true);
    }
    $FileLog = fopen('./Logs/'.$filename.".txt", "a");
    $escreve = fwrite($FileLog,$content."\n\n");
    fclose($FileLog );
}

function somenteNumeros($frase) {
    return preg_replace('/\D/', '', $frase);
}
function duplicidadesql($db, $sql){
    $query = selectpadraoconta($db, $sql);

    if($query > 0){
        return $query;
    }else{
        return null;
    }
}
function configApache(){
    // Obter a quantidade máxima de memória disponível para scripts PHP
    $memory_limit = ini_get('memory_limit');

    // Obter o tamanho máximo de upload permitido
    $upload_max_filesize = ini_get('upload_max_filesize');

    // Obter o tamanho máximo de dados permitidos em uma solicitação POST
    $post_max_size = ini_get('post_max_size');

    // Obter a versão do PHP
    $php_version = phpversion();

    // Exibir as informações
    echo "<h1>Configurações de PHP</h1>";
    echo "<p><strong>Versão do PHP:</strong> $php_version</p>";
    echo "<p><strong>Limite de Memória:</strong> $memory_limit</p>";
    echo "<p><strong>Tamanho Máximo de Upload:</strong> $upload_max_filesize</p>";
    echo "<p><strong>Tamanho Máximo de Dados POST:</strong> $post_max_size</p>";
}

function generateUUID() {
    // PHP 5.6 safe
    if (function_exists('openssl_random_pseudo_bytes')) {
        $data = openssl_random_pseudo_bytes(16);
    } else {
        // fallback (não criptográfico, mas evita fatal)
        $data = md5(uniqid(mt_rand(), true), true); // 16 bytes
    }

    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}