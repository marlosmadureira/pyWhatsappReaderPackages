<?php
    $db = null;

    include_once('funcao.php'); 

    configApache();

    setlocale(LC_TIME, 'pt_BR', 'pt_BR.utf-8', 'pt_BR.utf-8', 'portuguese');
    date_default_timezone_set('America/Sao_Paulo');
    mb_internal_encoding("UTF-8");

    set_time_limit(0);
    ini_set('memory_limit', '-1');

    echo '<form action="index.php" method="post" target="_self">  
                <label for="textfield">File Name:</label>
                <input type="text" name="fileName" size="80" value="'.$_POST['fileName'].'">  
                <button type="submit" name="Executar" value="Executar" class="btn btn-info text-center">Executar</button>         
            </form>';

    if(isset($_POST['Executar']) && !empty($_POST['fileName'])){
        //PRTT_Out_2741209949393128_43_5514982270440.json

        $caminhoArquivo = 'Logs/' . $_POST['fileName'];

        if (file_exists($caminhoArquivo)) {
            $extensao = pathinfo($caminhoArquivo, PATHINFO_EXTENSION);

            $type = null;

            if ($extensao === 'json') {
                if (strpos($caminhoArquivo, 'PRTT') !== false) {
                    $type = 'PRTT';
                }elseif (strpos($caminhoArquivo, 'DADOS') !== false){
                    $type = 'DADOS';
                }elseif (strpos($caminhoArquivo, 'GDADOS') !== false) {
                    $type = 'GDADOS';
                }

                $jsonData = file_get_contents($caminhoArquivo);

                if(!empty($type)){
                    $jsonRetorno = InsertBanco($db, $type, $jsonData);
                }else{
                    $jsonRetorno = null;
                }
            }
        }

        $status['fileName'] = $caminhoArquivo;
        $status['jsonRetorno'] = $jsonRetorno;
        $status['status'] = 200;
        $status['text'] = "ok"; 

        // Exibe todo o array de forma estruturada
        echo "<h3>Informações gerais</h3>";
        echo "<pre>";
            print_r($status);
        echo "</pre>";

        // Decodifica e exibe o jsonRetorno em formato JSON estruturado
        $jsonRetorno = json_decode($status["jsonRetorno"], true);

        echo "<h3>jsonRetorno Estruturado</h3>";
        echo "<pre>";
            echo json_encode($jsonRetorno, JSON_PRETTY_PRINT);
        echo "</pre>";
    }
    
    function InsertBanco($db, $type, $jsonString){

        $executaSql = True;                
        $logGrava = False;                   
        $printLogJson = False;
        $jsonRetorno['GravaBanco'] = null;

        $json = json_decode($jsonString);

        //EXTRAÇÃO DO CABEÇALHO DO PACOTE
        if(isset($json->FileName)){
            $FileName = trim(pg_escape_string($json->FileName));                
        }
        if(isset($json->Unidade)){
            $Unidade = trim(pg_escape_string($json->Unidade));              
        }
        if(isset($json->InternalTicketNumber)){
            $InternalTicketNumber = trim(pg_escape_string($json->InternalTicketNumber));
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
        if(isset($json->Service)){
            $Service = trim(pg_escape_string($json->Service));
        }
        
        $jsonRetorno['TypePRTTouDADOSouGDADOS'] = trim(pg_escape_string($type));
        $jsonRetorno['FileName'] = trim(pg_escape_string($FileName));
        $jsonRetorno['AccountIdentifier'] = trim(pg_escape_string($AccountIdentifier));
        $jsonRetorno['Unidade'] = trim(pg_escape_string($Unidade));
        $jsonRetorno['UnidName'] = find_unidade($db, $Unidade);
        $jsonRetorno['InternalTicketNumber'] = trim(pg_escape_string($InternalTicketNumber));

        if(!empty($AccountIdentifier) && $AccountIdentifier != '' && $AccountIdentifier != ' ' && !empty($Unidade) && $Unidade > 0){
            
            //TRATAMENTO DE CONTA WHATSAPP
            $sqlTratamento = "SELECT apli_id, linh_id, conta_id FROM linha_imei.tbaplicativo_linhafone WHERE status = 'A' AND apli_id = 1 AND conta_zap IS NULL;";
            $queryTratamento = selectpadraoumalinha($db,$sqlTratamento);

            if(!empty($queryTratamento['conta_id']) && $queryTratamento['conta_id'] > 0){
                $conta_id = trim(pg_escape_string(preg_replace('/[^0-9]/','',$queryTratamento['conta_id'])));
                $apli_id = trim($queryTratamento['apli_id']);
                $linh_id = trim($queryTratamento['linh_id']);
                $sqlUpdate = "UPDATE linha_imei.tbaplicativo_linhafone SET conta_zap = '".$conta_id."' WHERE conta_zap IS NULL AND apli_id = ".$apli_id." AND linh_id = ".$linh_id;

                if ($executaSql){
                    $resultEventoBd = null;
                    $resultEventoBd = alterarRegistro($db,$sqlUpdate);

                    if($printLogJson){
                        $jsonRetorno['1'] = 'OK ' . $resultEventoBd;
                        $jsonRetorno['1.1'] = $sqlTratamento;
                        $jsonRetorno['1.2'] = $sqlUpdate;
                    }
                } 

                if($logGrava){
                    gravalog($FileName, "1");
                    gravalog($FileName, $sqlUpdate); 
                }
            }
            
            //NOVO SQL PARA TRATAMENTO DE UNIDADES 23OUT23
            $sqllinh_id = "SELECT tbaplicativo_linhafone.linh_id FROM interceptacao.tbobje_intercepta, linha_imei.tbaplicativo_linhafone WHERE tbobje_intercepta.linh_id = tbaplicativo_linhafone.linh_id AND tbaplicativo_linhafone.apli_id = 1 AND tbaplicativo_linhafone.status = 'A' AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.unid_id = ".$Unidade." AND tbaplicativo_linhafone.conta_zap = '".$AccountIdentifier."' GROUP BY tbaplicativo_linhafone.linh_id;";
            $query = selectpadraoumalinha($db,$sqllinh_id);

            if($logGrava){
                gravalog($FileName, "2");
                gravalog($FileName, $sqllinh_id); 
            }

            if($printLogJson){
                $jsonRetorno['2'] = 'OK ' . $query['linh_id'];
                $jsonRetorno['2.1'] = $sqllinh_id;
            }

            if(!empty($query['linh_id']) && $query['linh_id'] > 0){

                $linh_id = $query['linh_id']; 

                //ARQUIVOS DO TIPO DADOS 
                if($type == "DADOS"){

                    $queryArId = null;

                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."'";
                    $repetido = selectpadraoumalinha($db, $sqlexistente);

                    if (empty($repetido['ar_id'])){
                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) VALUES (".$linh_id.", '".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 1, 1, '".$EmailAddresses."') RETURNING ar_id;";

                        if ($executaSql){
                            $queryArId = inserirRegistroReturning($db,$sqlInsert);

                            if($printLogJson){
                                $jsonRetorno['3'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                $jsonRetorno['3.1'] = $sqlexistente;
                                $jsonRetorno['3.2'] = $sqlInsert;
                            }

                        }

                        if($logGrava){
                            gravalog($FileName, "3");
                            gravalog($FileName, $sqlInsert);
                            gravalog($FileName, $sqlexistente); 
                        }
                        
                        if(!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0){

                            $ar_id = $queryArId['ar_id'];

                            if(isset($json->Dados->EmailAddresses)){
                                $EmailAddresses = trim(pg_escape_string($json->Dados->EmailAddresses));

                                if(!empty($EmailAddresses)){
                                    $sqlUpdate = "UPDATE leitores.tb_whatszap_arquivo SET ar_email_addresses = '". $EmailAddresses ."' WHERE ar_id = " . $ar_id;

                                    $resultEventoBd = null;
                                    $resultEventoBd = alterarRegistro($db,$sqlUpdate);
                                }
                            }

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
                                    
                                    if(!empty($dadoIPAddress) && !empty($dadoTime)){
                                        //GRAVANDO OS LOGS DE IP/TIME 
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) VALUES ('".$dadoIPAddress."', '".$dadoTime."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                        
                                        if ($executaSql){
                                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_iptime WHERE ip_ip = '".$dadoIPAddress."' AND linh_id = ".$linh_id." AND ip_tempo = '".$dadoTime ."' AND telefone = '".$AccountIdentifier."';";
                                            
                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['ar_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['4'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['4.1'] = $sqlInsert;
                                                    $jsonRetorno['4.2'] = $sqlexistente;
                                                }
                                            }
                                        }
                                    }

                                    if($logGrava){
                                        gravalog($FileName, "4");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $sqlexistente);
                                    }
                                }
                            }
                            
                            if(isset($json->Dados->connectionInfo)){
                                if(isset($json->Dados->connectionInfo->Servicestart)){
                                    $dadoServiceStart = trim(pg_escape_string($json->Dados->connectionInfo->Servicestart));
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
                                    $dadoPushName = null; 
                                }
                                if(isset($json->Dados->connectionInfo->LastSeen)){
                                    $dadoLastSeen = trim(pg_escape_string($json->Dados->connectionInfo->Lastseen));
                                }else{
                                    $dadoLastSeen = null; 
                                }
                                if(isset($json->Dados->connectionInfo->LastIP)){
                                    $LastIP = trim(pg_escape_string($json->Dados->connectionInfo->LastIP));
                                }else{
                                    $LastIP = null; 
                                }

                                if(!empty($dadoServiceStart)){
                                    //GRAVANDO CONEXÃO CONEXÃO INFO 
                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) VALUES ( '".$dadoServiceStart."', '".$dadoDeviceType."', '".$dadoAppVersion."', '".$dadoDeviceOSBuildNumber."', '".$dadoConnectionState."', '".$dadoOnlineSince."', '".$dadoPushName."', '".$dadoLastSeen."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                    
                                    if ($executaSql){
                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_conexaoinfo WHERE servicestart = '".$dadoServiceStart."' AND linh_id = ".$linh_id." AND devicetype = '".$dadoDeviceType."' AND appversion = '".$dadoAppVersion."' AND deviceosbuildnumber = '".$dadoDeviceOSBuildNumber."' AND telefone = '".$AccountIdentifier."';"; 

                                        $existente = selectpadraoumalinha($db, $sqlexistente);
                                        if(empty($existente['ar_id'])){
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                            if($printLogJson){
                                                $jsonRetorno['5'] = 'OK ' . $resultEventoBd;
                                                $jsonRetorno['5.1'] = $sqlInsert;
                                                $jsonRetorno['5.2'] = $sqlexistente;
                                            }
                                        }
                                    }
                                }

                                if($logGrava){
                                    gravalog($FileName, "5");
                                    gravalog($FileName, $sqlInsert);
                                    gravalog($FileName, $sqlexistente);
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
                                if(isset($json->Dados->webInfo->Availability)){
                                    $Availability = trim(pg_escape_string($json->Dados->webInfo->Availability));
                                }else{
                                    $Availability = null;
                                }

                                if(!empty($dadoVersion)){
                                    //GRAVANDO OS DADOS WEBINFO
                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) VALUES ('".$dadoVersion."', '".$dadoPlatform."', '".$dadoOnlineSince."', '".$dadoInactiveSince."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";                               
                                    
                                    if ($executaSql){
                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_weinfo WHERE we_version = '".$dadoVersion."' AND linh_id = ".$linh_id." AND we_platform = '".$dadoPlatform."' AND telefone = '".$AccountIdentifier."';"; 

                                        $existente = selectpadraoumalinha($db, $sqlexistente);
                                        if(empty($existente['ar_id'])){
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                            if($printLogJson){
                                                $jsonRetorno['6'] = 'OK ' . $resultEventoBd;
                                                $jsonRetorno['6.1'] = $sqlInsert;
                                                $jsonRetorno['6.2'] = $sqlexistente;
                                            }
                                        }
                                    }
                                }

                                if($logGrava){
                                    gravalog($FileName, "6");
                                    gravalog($FileName, $sqlInsert);
                                    gravalog($FileName, $sqlexistente);
                                }
                            }
                            
                            // CORRIGIDO PARA PADRAO NOVO
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

                                    if(!empty($dadoID)){
                                        //GRAVANDO INFORMAÇÕES DO GRUPO OWNED 
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('".$dadoTipoGroup."', '".$pathFile."', '".$dadoThumbnail."', '".$dadoID."', '".$dadoCreation."', '".$dadoSize."', '".$dadoDescription."', '".$dadoSubject."', '".$AccountIdentifier."', ".$ar_id.", '".$dadoPicture."', ".$linh_id.");";
                                       
                                        if ($executaSql){
                                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '".$dadoTipoGroup."' AND linh_id = ".$linh_id." AND creation = '".$dadoCreation."' AND id_msg = '".$dadoID."' AND telefone = '".$AccountIdentifier."';"; 

                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['ar_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['7'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['7.1'] = $sqlInsert;
                                                    $jsonRetorno['7.2'] = $sqlexistente;
                                                }
                                            }
                                        }

                                    }

                                    if($logGrava){
                                        gravalog($FileName, "7");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $sqlexistente);
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

                                    if(!empty($dadoID)){
                                        //GRAVANDO INFORMAÇÕES DO GRUPO PARTICIPATING 
                                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('".$dadoTipoGroup."', '".$pathFile."', '".$dadoThumbnail."', '".$dadoID."', '".$dadoCreation."', '".$dadoSize."', '".$dadoDescription."', '".$dadoSubject."', '".$AccountIdentifier."', ".$ar_id.", '".$dadoPicture."', ".$linh_id.");";
                                        
                                        if ($executaSql){
                                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '".$dadoTipoGroup."' AND linh_id = ".$linh_id." AND creation = '".$dadoCreation."' AND id_msg = '".$dadoID."' AND telefone = '".$AccountIdentifier."';"; 

                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['ar_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['8'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['8.1'] = $sqlInsert;
                                                    $jsonRetorno['8.2'] = $sqlexistente;
                                                }
                                            }
                                        }
                                    }

                                    if($logGrava){
                                        gravalog($FileName, "8");
                                        gravalog($FileName, $sqlInsert);
                                        gravalog($FileName, $sqlexistente);
                                    }
                                }
                            }
                            
                            // CORRIGIDO PARA PADRAO NOVO
                            if(isset($json->Dados->addressBookInfo)){
                                if(isset($json->Dados->addressBookInfo[0]->Symmetriccontacts)){ 
                                    foreach($json->Dados->addressBookInfo[0]->Symmetriccontacts as $registro){
                                        $dadosymmetricContacts = trim(pg_escape_string($registro));

                                        //GRAVANDO TELEFONES SINCRONA
                                        if(isset($dadosymmetricContacts) && !empty($dadosymmetricContacts)){ 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('".$dadosymmetricContacts."', 'S', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '".$dadosymmetricContacts."' AND linh_id = ".$linh_id." AND ag_tipo = 'S' AND telefone = '".$AccountIdentifier."';"; 

                                                $existente = selectpadraoumalinha($db, $sqlexistente);
                                                if(empty($existente['ar_id'])){
                                                    $resultEventoBd = null;
                                                    $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                    if($printLogJson){
                                                        $jsonRetorno['9'] = 'OK ' . $resultEventoBd;
                                                        $jsonRetorno['9.1'] = $sqlInsert;
                                                        $jsonRetorno['9.2'] = $sqlexistente;
                                                    }
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "9");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }
                                        }
                                    }
                                }
                                
                                if(isset($json->Dados->addressBookInfo[0]->Asymmetriccontacts)){ 

                                    foreach($json->Dados->addressBookInfo[0]->Asymmetriccontacts as $registro){
                                        $dadoasymmetricContacts = trim(pg_escape_string($registro)); 

                                        //GRAVANDO TELEFONES ASINCRONA
                                        if(isset($dadoasymmetricContacts) && !empty($dadoasymmetricContacts)){ 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('".$dadoasymmetricContacts."', 'A', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '".$dadoasymmetricContacts."' AND linh_id = ".$linh_id." AND ag_tipo = 'A' AND telefone = '".$AccountIdentifier."';"; 

                                                $existente = selectpadraoumalinha($db, $sqlexistente);
                                                if(empty($existente['ar_id'])){
                                                    $resultEventoBd = null;
                                                    $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                    if($printLogJson){
                                                        $jsonRetorno['10'] = 'OK ' . $resultEventoBd;
                                                        $jsonRetorno['10.1'] = $sqlInsert;
                                                        $jsonRetorno['10.2'] = $sqlexistente;
                                                    }
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "10");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }
                                        }
                                    }
                                }
                            }

                            if(isset($json->Dados->smallmediumbusinessinfo)){
                                //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
                                $dadosmallMediumBusiness = trim(pg_escape_string($json->Dados->smallmediumbusinessinfo));   

                                if($printLogJson){
                                    $jsonRetorno['18'] = 'OK FALTA FAZER ' . $dadosmallMediumBusiness; 
                                }                   
                            }

                            if(isset($json->Dados->ncmecReportsInfo)){
                                //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
                                if(isset($json->Dados->ncmecReportsInfo->NcmecReportsDefinition)){
                                    $NcmecReportsDefinition = trim(pg_escape_string($json->Dados->ncmecReportsInfo->NcmecReportsDefinition));
                                }else{
                                    $NcmecReportsDefinition = null;
                                }

                                if(isset($json->Dados->ncmecReportsInfo->NCMECCyberTipNumbers)){                        
                                    $NCMECCyberTipNumbers = trim(pg_escape_string($json->Dados->ncmecReportsInfo->NCMECCyberTipNumbers));
                                }else{
                                    $NCMECCyberTipNumbers = null;
                                }

                                if($printLogJson){
                                    $jsonRetorno['19'] = 'OK FALTA FAZER ' . $NCMECCyberTipNumbers;
                                }
                            }

                            if(isset($json->Dados->deviceinfo)){
                                //AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
                                if(isset($json->Dados->deviceinfo->AppVersion)){
                                    $AppVersion = trim(pg_escape_string($json->Dados->deviceinfo->AppVersion));
                                }else{
                                    $AppVersion = null;
                                }

                                if(isset($json->Dados->deviceinfo->OSVersion)){             
                                    $OSVersion = trim(pg_escape_string($json->Dados->deviceinfo->OSVersion));
                                }else{
                                    $OSVersion = null;
                                }

                                if(isset($json->Dados->deviceinfo->OSBuildNumber)){
                                    $OSBuildNumber = trim(pg_escape_string($json->Dados->deviceinfo->OSBuildNumber));                       
                                }else{
                                    $OSBuildNumber = null;
                                }

                                if(isset($json->Dados->deviceinfo->DeviceManufacturer)){
                                    $DeviceManufacturer = trim(pg_escape_string($json->Dados->deviceinfo->DeviceManufacturer));
                                }else{
                                    $DeviceManufacturer = null;
                                }

                                if(isset($json->Dados->deviceinfo->DeviceModel)){
                                    $DeviceModel = trim(pg_escape_string($json->Dados->deviceinfo->DeviceModel));    
                                }else{
                                    $DeviceModel = null;
                                }

                                if(!empty($AppVersion)){
                                    //GRAVANDO INFORMAÇÕES DO GRUPO PARTICIPATING 
                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_deviceinfo (dev_appversion, dev_osversion, dev_buildnumber, dev_manufacturer, dev_devicemodel, ar_id, linh_id, telefone) VALUES ('".$AppVersion."', '".$OSVersion."', '".$OSBuildNumber."', '".$DeviceManufacturer."', '".$DeviceModel."', ".$ar_id.", ".$linh_id.", '".$AccountIdentifier."');";
                                    
                                    if ($executaSql){
                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_deviceinfo WHERE dev_appversion = '".$AppVersion."' AND linh_id = ".$linh_id." AND dev_osversion = '".$OSVersion."' AND telefone = '".$AccountIdentifier."';"; 

                                        $existente = selectpadraoumalinha($db, $sqlexistente);
                                        if(empty($existente['ar_id'])){
                                            $resultEventoBd = null;
                                            $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                            if($printLogJson){
                                                $jsonRetorno['20'] = 'OK ' . $resultEventoBd;
                                                $jsonRetorno['20.1'] = $sqlInsert;
                                                $jsonRetorno['20.2'] = $sqlexistente;
                                            }
                                        }
                                    }
                                }

                                if($logGrava){
                                    gravalog($FileName, "20");
                                    gravalog($FileName, $sqlInsert);
                                    gravalog($FileName, $sqlexistente);
                                }
                            }
                        }
                        $jsonRetorno['GravaBanco'] = True;

                        $FileLog = fopen("ArquivoProcessados.txt", "a");        
                        $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");   
                        fclose($FileLog ); 

                    }else{
                        $jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
                        $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");       
                        $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . " Arquivo Existente \n\n");    
                        fclose($FileLog ); 
                        $jsonRetorno['GravaBanco'] = False;
                    }
                }
                                     
                //ARQUIVOS DO TIPO PRTT 
                if($type == "PRTT"){

                    $queryArId = null;

                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."';";
                    $repetido = selectpadraoconta($db, $sqlexistente);

                    if (empty($repetido['ar_id'])){
                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id) VALUES ('".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 0, 1, ".$linh_id.") RETURNING ar_id;";

                        if ($executaSql){
                            $queryArId = inserirRegistroReturning($db,$sqlInsert);

                            if($printLogJson){
                                $jsonRetorno['11'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                $jsonRetorno['11.1'] = $sqlInsert;
                                $jsonRetorno['11.2'] = $sqlexistente;
                            }
                        }

                        if($logGrava){
                            gravalog($FileName, "11");
                            gravalog($FileName, $sqlInsert);
                            gravalog($FileName, $sqlexistente);
                        }
                        
                        if(!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0){

                            $ar_id = $queryArId['ar_id'];
                            
                            //PRTT DE MENSSAGENS
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

                                    if(empty($prttGroupId)){

                                        //VERIFICAÇÃO PARA INSERIR AS TROCAS DE MENSAGENS INDIVIDUAL
                                        if($prttSender == $AccountIdentifier){

                                            $TipoDirecaoMsg = "Enviou"; 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttSender."', '".$prttRecipients."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['12'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['12.1'] = $sqlInsert;
                                                    $jsonRetorno['12.2'] = $sqlexistente;
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "12");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }

                                        }else{

                                            $TipoDirecaoMsg = "Recebeu"; 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['13'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['13.1'] = $sqlInsert;
                                                    $jsonRetorno['13.2'] = $sqlexistente;
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "13");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }
                                        }   

                                    }else{

                                        //VERIFICAÇÃO PARA INSERIR AS TROCAS DE MENSAGENS GROUP
                                        if($prttSender == $AccountIdentifier){

                                            $TipoDirecaoMsg = "Enviou"; 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttSender."', '".$prttRecipients."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['14'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['14.1'] = $sqlInsert;
                                                    $jsonRetorno['14.2'] = $sqlexistente;
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "14");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }

                                        }else{

                                            $TipoDirecaoMsg = "Recebeu"; 
                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
                                            
                                            if ($executaSql){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                if($printLogJson){
                                                    $jsonRetorno['15'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['15.1'] = $sqlInsert;
                                                    $jsonRetorno['15.2'] = $sqlexistente;
                                                }
                                            }

                                            if($logGrava){
                                                gravalog($FileName, "15");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente);
                                            }
                                        }
                                    }                       
                                }
                            }

                            //PRTT LOG DE CHAMADAS
                            if(isset($json->Prtt->callLogs)){

                                foreach($json->Prtt->callLogs as $registro){

                                    if(isset($registro->CallId)){
                                        $prttcallID = trim(pg_escape_string($registro->CallId));
                                    }else{
                                        $prttcallID = null;
                                    }
                                    if(isset($registro->CallCreator)){
                                        $prttcallCreator = trim(pg_escape_string($registro->CallCreator));
                                    }else{
                                        $prttcallCreator = null;
                                    }                       

                                    if(isset($registro->Events)){

                                        foreach($registro->Events as $subregistro){
                                            if(isset($subregistro->Type)){
                                                $prttEtype = trim(pg_escape_string($subregistro->Type));
                                            }else{
                                                $prttEtype = null;
                                            }
                                            if(isset($subregistro->Timestamp)){
                                                $prttEtimestamp = trim(pg_escape_string(str_replace("UTC","",$subregistro->Timestamp)));
                                            }else{
                                                $prttEtimestamp = null;
                                            }
                                            if(isset($subregistro->From)){
                                                $prttEsolicitante = trim(pg_escape_string($subregistro->From));
                                            }else{
                                                $prttEsolicitante = null;
                                            }
                                            if(isset($subregistro->To)){
                                                $prttEatendente = trim(pg_escape_string($subregistro->To));
                                            }else{
                                                $prttEatendente = null;
                                            }
                                            if(isset($subregistro->FromIp)){
                                                $prttEsolIP = trim(pg_escape_string($subregistro->FromIp));
                                            }else{
                                                $prttEsolIP = null;
                                            }
                                            if(isset($subregistro->FromPort)){
                                                $prttEsolPort = trim(pg_escape_string($subregistro->FromPort));
                                            }else{
                                                $prttEsolPort = 0;
                                            }
                                            if(isset($subregistro->MediaType)){
                                                $prttEmediaType = trim(pg_escape_string($subregistro->MediaType));
                                            }else{
                                                $prttEmediaType = null;
                                            }
                                            if($prttcallCreator == $AccountIdentifier){
                                                $TipoDirecaoCall = "EFETUOU";
                                            }else{
                                                $TipoDirecaoCall = "RECEBEU";
                                            }
                                            if(isset($subregistro->PhoneNumber)){
                                                $prttPhoneNumber = trim(pg_escape_string($subregistro->PhoneNumber));
                                            }else{
                                                $prttPhoneNumber = null;
                                            }

                                            if(count($subregistro->Participants) > 0){
                                                if (isset($subregistro->Participants)){
                                                    foreach($subregistro->Participants as $eventParticipant){
                                                        if(isset($eventParticipant->PhoneNumber)){
                                                            $prttPhoneNumber = $eventParticipant->PhoneNumber;
                                                        }else{
                                                            $prttPhoneNumber = null;
                                                        }

                                                        // Array para armazenar os estados encontrados
                                                        $estadosEncontrados = array();

                                                        // Verifique se cada estado específico está presente na prttEtimestamp
                                                        $estadosEspecíficos = array("Stateinvited", "Statereceipt", "Stateconnected", "Stateoutgoing", "StateconnectedPlatformandro", "ParticipantsPhone");
                                                        foreach ($estadosEspecíficos as $estado) {
                                                            if (strpos($prttEtimestamp, $estado) !== false) {
                                                                $estadosEncontrados[] = $estado;
                                                            }
                                                        }

                                                        if (empty($estadosEncontrados)) {
                                                            //INSERT DE CHAMADAS TROCADAS EM ALVO/INTERLOCUTOR 
                                                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES ( '".$prttcallID."', '".$prttcallCreator."', '".$prttEtype."', '".$prttEtimestamp."', '".$prttEsolicitante."', '".$prttEatendente."', '".$prttEsolIP."', '".$prttEsolPort."', '".$prttEmediaType."', '".$prttPhoneNumber."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.", '".$TipoDirecaoCall."');";
                                                           
                                                            if ($executaSql){
                                                                $resultEventoBd = null;
                                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                                if($printLogJson){
                                                                    $jsonRetorno['16'] = 'OK ' . $resultEventoBd;
                                                                    $jsonRetorno['16.1'] = $sqlInsert;
                                                                    $jsonRetorno['16.2'] = $sqlexistente;
                                                                }
                                                            } 
                                                        }

                                                        if($logGrava){
                                                            gravalog($FileName, "16");
                                                            gravalog($FileName, $sqlInsert);
                                                            gravalog($FileName, $sqlexistente);
                                                        }
                                                    }
                                                }

                                            }else{      
                                                // Verifique se cada estado específico está presente na prttEtimestamp
                                                $estadosEspecíficos = array("Stateinvited", "Statereceipt", "Stateconnected", "Stateoutgoing", "StateconnectedPlatformandro", "ParticipantsPhone");
                                                foreach ($estadosEspecíficos as $estado) {
                                                    if (strpos($prttEtimestamp, $estado) !== false) {
                                                        $estadosEncontrados[] = $estado;
                                                    }
                                                }

                                                if (empty($estadosEncontrados)) {
                                                    //INSERT DE CHAMADAS TROCADAS EM ALVO/INTERLOCUTOR 
                                                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES ('".$prttcallID."', '".$prttcallCreator."', '".$prttEtype."', '".$prttEtimestamp."', '".$prttEsolicitante."', '".$prttEatendente."', '".$prttEsolIP."', '".$prttEsolPort."', '".$prttEmediaType."', '".$prttPhoneNumber."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.", '".$TipoDirecaoCall."');";
                                                    
                                                    if ($executaSql){
                                                        $resultEventoBd = null;
                                                        $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                        if($printLogJson){
                                                            $jsonRetorno['17'] = 'OK ' . $resultEventoBd;
                                                            $jsonRetorno['17.1'] = $sqlInsert;
                                                            $jsonRetorno['17.2'] = $sqlexistente;
                                                        }
                                                    }
                                                }

                                                if($logGrava){
                                                    gravalog($FileName, "17");
                                                    gravalog($FileName, $sqlInsert);
                                                    gravalog($FileName, $sqlexistente);
                                                }
                                            }
                                        }
                                    }else{
                                        $jsonRetorno['ErroCall'] = 'Erro Events Call';
                                    }
                                }
                            }

                            $jsonRetorno['GravaBanco'] = True;

                            $FileLog = fopen("ArquivoProcessados.txt", "a");        
                            $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");   
                            fclose($FileLog ); 
                        }
                        
                    }else{
                        $jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
                        $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");       
                        $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . "\nArquivo Existente \n\n");   
                        fclose($FileLog ); 
                        $jsonRetorno['GravaBanco'] = False;
                    }
                }

                //ARQUIVO DO TIPO SEM TAG DADOS OU PRTT
                if(empty($type) || $type == ''){
                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."';";
                    $repetido = selectpadraoconta($db, $sqlexistente);

                    if (empty($repetido['ar_id'])){
                        $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id) VALUES ('".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 0, 1, ".$linh_id.") RETURNING ar_id;";

                        if ($executaSql){
                            $queryArId = inserirRegistroReturning($db,$sqlInsert);

                            if($printLogJson){
                                $jsonRetorno['21'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                $jsonRetorno['21.1'] = $sqlInsert;
                                $jsonRetorno['21.2'] = $sqlexistente;
                            }

                        }

                        if($logGrava){
                            gravalog($FileName, "21");
                            gravalog($FileName, $sqlInsert);
                            gravalog($FileName, $sqlexistente); 
                        }
                    }

                    $jsonRetorno['GravaBanco'] = True;

                    $FileLog = fopen("ArquivoProcessados.txt", "a");        
                    $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");   
                    fclose($FileLog ); 
                }

                $jsonRetorno['MostraJsonPython'] = False;
                $jsonRetorno['RetornoPHP'] = True;
                $jsonRetorno['ExibirTotalPacotesFila'] = False;
                $jsonRetorno['DataHora'] = date('Y-m-d H:i:s');

            }else{

                //ARQUIVOS DO TIPO DADOS 
                if($type == "GDADOS"){

                    $sqlGrupo = "SELECT tbobje_whatsappgrupos.grupo_id, tbobje_intercepta.linh_id, tbobje_intercepta.obje_id FROM interceptacao.tbobje_whatsappgrupos, interceptacao.tbobje_intercepta WHERE tbobje_intercepta.obje_id = tbobje_whatsappgrupos.obje_id AND tbobje_intercepta.opra_id = 28 AND tbobje_intercepta.unid_id = ".$Unidade." AND tbobje_whatsappgrupos.grupo_id ILIKE '%".$AccountIdentifier."%'";
                    $queryGrupo= selectpadraoumalinha($db,$sqlGrupo);

                    if(!empty($queryGrupo['linh_id']) && $queryGrupo['linh_id'] > 0){
                        $queryArId = null;

                        $linh_id = $queryGrupo['linh_id'];

                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 2 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."'";
                        $repetido = selectpadraoumalinha($db, $sqlexistente);

                        if (empty($repetido['ar_id'])){
                            $sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status) VALUES (".$linh_id.", '".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 2, 1) RETURNING ar_id;";

                            if ($executaSql){
                                $queryArId = inserirRegistroReturning($db,$sqlInsert);

                                if($printLogJson){
                                    $jsonRetorno['1G'] = 'OK FILE BANCO ' . $queryArId['ar_id'];
                                    $jsonRetorno['1G.1'] = $sqlInsert;
                                    $jsonRetorno['1G.2'] = $sqlexistente;
                                }
                            }

                            if($logGrava){
                                gravalog($FileName, "1G");
                                gravalog($FileName, $sqlInsert);
                                gravalog($FileName, $sqlexistente); 
                            }
                            
                            if(!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0){
                                $ar_id = $queryArId['ar_id'];

                                $sqlIdentificador = "SELECT tbmembros_whats.identificador FROM whatsapp.tbmembros_whats, whatsapp.tbgrupowhatsapp, linha_imei.tbaplicativo_linhafone, linha_imei.tblinhafone WHERE  tbmembros_whats.grupo_id = tbgrupowhatsapp.grupo_id AND tbmembros_whats.identificador = tbaplicativo_linhafone.identificador AND tblinhafone.linh_id = tbaplicativo_linhafone.linh_id AND tblinhafone.unid_id = ".$Unidade." AND tbmembros_whats.grupo_id ILIKE '%".trim($AccountIdentifier)."%'";
                                $queryIdentificador = selectpadraoumalinha($db, $sqlIdentificador);

                                if(isset($json->GDados->groupsInfo) && !empty($queryIdentificador['identificador'])){  
                                    $identificador = $queryIdentificador['identificador'];

                                    foreach($json->GDados->groupsInfo->GroupParticipants as $registro){
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES ('".trim($AccountIdentifier)."', '".somenteNumeros($registro)."', 'N', 'A', ".$identificador.");";
                                        
                                        if ($executaSql){
                                            $sqlexistente = "SELECT grupo_id FROM whatsapp.tbmembros_whats WHERE grupo_id ILIKE '%".$AccountIdentifier."%' AND grupo_participante = '".somenteNumeros($registro)."'"; 

                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['grupo_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                //$sqlInsertAgenda = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, ar_id, linh_id) VALUES ('".somenteNumeros($registro)."', 'G', ".$ar_id.", ".$linh_id.");";

                                                if($printLogJson){
                                                    $jsonRetorno['2G'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['2G.1'] = $sqlInsert;
                                                    $jsonRetorno['2G.2'] = $sqlexistente;
                                                    $jsonRetorno['2G.3'] = $sqlIdentificador;
                                                }
                                            } 

                                            if($logGrava){
                                                gravalog($FileName, "2G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente); 
                                            }
                                        }
                                    }

                                    foreach($json->GDados->groupsInfo->GroupAdministrators as $registro){
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES ('".trim($AccountIdentifier)."', '".somenteNumeros($registro)."', 'S', 'A', ".$identificador.");";
                                        
                                        if ($executaSql){
                                            $sqlexistente = "SELECT grupo_id FROM whatsapp.tbmembros_whats WHERE grupo_id ILIKE '%".$AccountIdentifier."%' AND grupo_participante = '".somenteNumeros($registro)."' AND grupo_adm = 'S'"; 

                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['grupo_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                //$sqlInsertAgenda = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, ar_id, linh_id) VALUES ('".somenteNumeros($registro)."', 'G', ".$ar_id.", ".$linh_id.");";

                                                if($printLogJson){
                                                    $jsonRetorno['3G'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['3G.1'] = $sqlInsert;
                                                    $jsonRetorno['3G.2'] = $sqlexistente;
                                                    $jsonRetorno['3G.3'] = $sqlIdentificador;
                                                }
                                            } 

                                            if($logGrava){
                                                gravalog($FileName, "3G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente); 
                                            }
                                        }
                                    }

                                    foreach($json->GDados->groupsInfo->Participants as $registro){
                                        //GRAVANDO PARTICIPANTES GRUPO
                                        $sqlInsert = "INSERT INTO whatsapp.tbmembros_whats (grupo_id, grupo_participante, grupo_adm, grupo_status, identificador) VALUES ('".trim($AccountIdentifier)."', '".somenteNumeros($registro)."', 'N', 'A', ".$identificador.");";
                                        
                                        if ($executaSql){
                                            $sqlexistente = "SELECT grupo_id FROM whatsapp.tbmembros_whats WHERE grupo_id ILIKE '%".$AccountIdentifier."%' AND grupo_participante = '".somenteNumeros($registro)."'"; 

                                            $existente = selectpadraoumalinha($db, $sqlexistente);
                                            if(empty($existente['grupo_id'])){
                                                $resultEventoBd = null;
                                                $resultEventoBd = inserirRegistro($db,$sqlInsert);

                                                //$sqlInsertAgenda = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, ar_id, linh_id) VALUES ('".somenteNumeros($registro)."', 'G', ".$ar_id.", ".$linh_id.");";

                                                if($printLogJson){
                                                    $jsonRetorno['4G'] = 'OK ' . $resultEventoBd;
                                                    $jsonRetorno['4G.1'] = $sqlInsert;
                                                    $jsonRetorno['4G.2'] = $sqlexistente;
                                                    $jsonRetorno['4G.3'] = $sqlIdentificador;
                                                }
                                            } 

                                            if($logGrava){
                                                gravalog($FileName, "4G");
                                                gravalog($FileName, $sqlInsert);
                                                gravalog($FileName, $sqlexistente); 
                                            }
                                        }
                                    } 

                                    $FileLog = fopen("ArquivoProcessados.txt", "a");        
                                    $escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . "\n\n");   
                                    fclose($FileLog ); 

                                    $jsonRetorno['GravaBanco'] = True;
                                    $jsonRetorno['MostraJsonPython'] = False;
                                    $jsonRetorno['RetornoPHP'] = True;
                                    $jsonRetorno['ExibirTotalPacotesFila'] = False;
                                    $jsonRetorno['DataHora'] = date('Y-m-d H:i:s');
                                }else{
                                    $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");       
                                    $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \n" . $sqllinh_id ."\nLinha Nao Localizada \n\n");   
                                    fclose($FileLog ); 

                                    $jsonRetorno['GravaBanco'] = False;
                                    $jsonRetorno['AVISO_1G'] = 'GRUPO Nao Localizada ' . $AccountIdentifier;
                                }
                            }
                        }
                    }else{
                        $FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a");       
                        $escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \n" . $sqllinh_id ."\nLinha Nao Localizada \n\n");   
                        fclose($FileLog ); 

                        $jsonRetorno['GravaBanco'] = False;
                        $jsonRetorno['AVISO_1'] = 'Linha Id Nao Localizada ' . $AccountIdentifier;
                    }
                }
            }               
        }

        return json_encode($jsonRetorno);
        
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
        $FileLog = fopen('./logs/'.$filename.".txt", "a");  
        $escreve = fwrite($FileLog,$content."\n\n");    
        fclose($FileLog ); 
    }

    function somenteNumeros($frase) { 
        return preg_replace('/\D/', '', $frase);
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
        echo "<h3>Configurações de PHP</h3>";
        echo "<p><strong>Versão do PHP:</strong> $php_version</p>";
        echo "<p><strong>Limite de Memória:</strong> $memory_limit</p>";
        echo "<p><strong>Tamanho Máximo de Upload:</strong> $upload_max_filesize</p>";
        echo "<p><strong>Tamanho Máximo de Dados POST:</strong> $post_max_size</p>";
    }

    function generateUUID() {
        // Gera 16 bytes (128 bits) de dados aleatórios
        $data = random_bytes(16);

        // Define as versões e variantes do UUID
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40); // Define a versão 4 (0100)
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80); // Define a variante (10xx)

        // Converte os bytes em uma string hexadecimal
        return sprintf(
            '%08x-%04x-%04x-%02x%02x-%012x',
            // Divida os bytes conforme o formato UUID
            bin2hex(substr($data, 0, 4)),  // 8 dígitos
            bin2hex(substr($data, 4, 2)),  // 4 dígitos
            bin2hex(substr($data, 6, 2)),  // 4 dígitos
            bin2hex(substr($data, 8, 1)),  // 2 dígitos
            bin2hex(substr($data, 9, 1)),  // 2 dígitos
            bin2hex(substr($data, 10, 6))  // 12 dígitos
        );
    }
?>
