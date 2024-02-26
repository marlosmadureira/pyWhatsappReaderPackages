<?php
    $db = null;
	include_once('funcao.php');

	$tokenAuthorized = "2021010251552-Chupisco2"; 

	setlocale(LC_TIME, 'pt_BR', 'pt_BR.utf-8', 'pt_BR.utf-8', 'portuguese');
	date_default_timezone_set('America/Sao_Paulo');
	mb_internal_encoding("UTF-8");

	set_time_limit(0);
	ini_set('memory_limit', '-1');

	function gravalog($filename,$content){
		$filename = str_replace(".zip", '', $filename);
		$FileLog = fopen('./Logs/'.$filename.".txt", "a"); 	//CRIANDO ARQUVIVO
		$escreve = fwrite($FileLog,$content."\n\n");	//ESCREVE NO ARQUIVO LOG
		fclose($FileLog ); //FIM DE LOG
	}

    function duplicidadesql($db, $sql){
        $query = selectpadraoconta($db, $sql);

        if($query > 0){
            return $query;
        }else{
            return null;
        }
    }

	if(!empty($_POST['token']) && $_POST['token'] == $tokenAuthorized){

		if($_POST['action'] == "updateStatus"){
			$sqlUpdate = "UPDATE configuracao.tblogjava SET log_data = now() WHERE log_id = 1 AND log_status = 1";
			alterarRegistro($db,$sqlUpdate);
			
			$status['status'] = 200;
			$status['text'] = "ok";

			echo json_encode($status);
		}

		if($_POST['action'] == "sendWPData"){

			if (file_exists('jsonWhatsappDados.txt')){
				unlink('jsonWhatsappDados.txt');
			}

			if (file_exists('jsonWhatsappPRTT.txt')){
				unlink('jsonWhatsappPRTT.txt');
			}

			if(!empty($_POST['jsonData'])){

				$jsonData = json_decode($_POST['jsonData'], true);

				if(!empty($jsonData)){

					if($_POST['type'] == "DADOS"){
						//$myfile = fopen("jsonWhatsappDados.txt", "w+") or die("Unable to open file!");
						$status['type'] = "DADOS";
					}

					if($_POST['type'] == "PRTT"){
						//$myfile = fopen("jsonWhatsappPRTT.txt", "w+") or die("Unable to open file!");
						$status['type'] = "PRTT";
					}

					$jsonRetorno = InsertBanco($db, $_POST['type'], $_POST['jsonData']);
					
					//fwrite($myfile, json_encode($jsonData));
					//fclose($myfile);
				}
			}

			//$status['jsonData'] = $jsonData;
			$status['jsonRetorno'] = $jsonRetorno;
			$status['status'] = 200;
			$status['text'] = "ok";

			echo json_encode($status);
		}
	}

	function InsertBanco($db, $type, $jsonData){

		$executaSql = True;  			//EXCECUTAR COMANDOS SQL
		$logGrava = False;				//GRAVAR LOGS DE SQL ARQUIVO TXT
		$printLogJson = True;

		if(isset($jsonData)){

			//CONVERTENDO EM JSON
			$json = json_decode($jsonData);

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
	        if(isset($json->EmailAddresses)){
	            $EmailAddresses = trim(pg_escape_string($json->EmailAddresses));
	        }
	        
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
						alterarRegistro($db,$sqlUpdate);

						if($printLogJson){
							$jsonRetorno['1'] = 'OK';
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
					$jsonRetorno['2'] = 'OK';
				}

				if(!empty($query['linh_id']) && $query['linh_id'] > 0){

					$linh_id = $query['linh_id']; 

					//ARQUIVOS DO TIPO DADOS 
					if($type == "Dados"){

						$sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."'";
						$repetido = selectpadraoconta($db, $sqlexistente);

						if (empty($repetido['ar_id'])){
							$sqlInsert = "INSERT INTO leitores.tb_whatszap_arquivo (linh_id, telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_email_addresses) VALUES (".$linh_id.", '".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 1, 1, '".$EmailAddresses."') RETURNING ar_id;";

							if ($executaSql){
	                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 1 AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."'";
	                            $existente = duplicidadesql($db, $sql);
	                            if(empty($existente)){
									$queryArId = inserirRegistroReturning($db,$sqlInsert);

									if($printLogJson){
										$jsonRetorno['3'] = 'OK';
									}
								}
							}

							if($logGrava){
								gravalog($FileName, "3");
								gravalog($FileName, $sqlInsert);
								gravalog($FileName, $existente . ' - ' . $sqlexistente); 
							}
							
							if(!empty($queryArId['ar_id']) && $queryArId['ar_id'] > 0){

								$ar_id = $queryArId['ar_id'];

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
					                    //GRAVANDO OS LOGS DE IP/TIME 
					                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_iptime (ip_ip, ip_tempo, telefone, ar_id, linh_id) VALUES ('".$dadoIPAddress."', '".$dadoTime."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
					                    if ($executaSql){
	                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_iptime WHERE ip_ip = '".$dadoIPAddress."' AND ip_tempo = '".$dadoTime ."' AND telefone = '".$AccountIdentifier."';";
	                                        $existente = duplicidadesql($db, $sql);
	                                        if(empty($existente)){
					                    		inserirRegistro($db,$sqlInsert);

					                    		if($printLogJson){
													$jsonRetorno['4'] = 'OK';
												}
					                    	}
					                    }

					                    if($logGrava){
						                    gravalog($FileName, "4");
						                    gravalog($FileName, $sqlInsert);
						                    gravalog($FileName, $existente . ' - ' . $sqlexistente);
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
						            	$dadoPushName = null; 
						            }
						            if(isset($json->Dados->connectionInfo->LastSeen)){
						                $dadoLastSeen = trim(pg_escape_string($json->Dados->connectionInfo->LastSeen));
						            }else{
						            	$dadoLastSeen = null; 
						            }
					                //GRAVANDO CONEXÃO CONEXÃO INFO 
					                $sqlInsert = "INSERT INTO leitores.tb_whatszap_conexaoinfo (servicestart, devicetype, appversion, deviceosbuildnumber, connectionstate, onlinesince, pushname, lastseen, telefone, ar_id, linh_id) VALUES ( '".$dadoServiceStart."', '".$dadoDeviceType."', '".$dadoAppVersion."', '".$dadoDeviceOSBuildNumber."', '".$dadoConnectionState."', '".$dadoOnlineSince."', '".$dadoPushName."', '".$dadoLastSeen."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
					                if ($executaSql){
	                                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_conexaoinfo WHERE servicestart = '".$dadoServiceStart."' AND devicetype = '".$dadoDeviceType."' AND appversion = '".$dadoAppVersion."' AND deviceosbuildnumber = '".$dadoDeviceOSBuildNumber."' AND telefone = '".$AccountIdentifier."';";
	                                    $existente = duplicidadesql($db, $sql);
	                                    if(empty($existente)){
					                		inserirRegistro($db,$sqlInsert);

					                		if($printLogJson){
												$jsonRetorno['5'] = 'OK';
											}
					                	}
					                }

					                if($logGrava){
						                gravalog($FileName, "5");
						                gravalog($FileName, $sqlInsert);
						                gravalog($FileName, $existente . ' - ' . $sqlexistente);
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
					                //GRAVANDO OS DADOS WEBINFO
					                $sqlInsert = "INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) SELECT '".$dadoVersion."', '".$dadoPlatform."', '".$dadoOnlineSince."', '".$dadoInactiveSince."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id." WHERE NOT EXISTS (SELECT ar_id FROM leitores.tb_whatszap_weinfo WHERE we_version = '".$dadoVersion."' AND we_platform = '".$dadoPlatform."' AND telefone = '".$AccountIdentifier."');";
					                $sqlInsert = "INSERT INTO leitores.tb_whatszap_weinfo (we_version, we_platform, we_onlinesince, we_inactivesince, telefone, ar_id, linh_id) VALUES ('".$dadoVersion."', '".$dadoPlatform."', '".$dadoOnlineSince."', '".$dadoInactiveSince."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";				                
					                if ($executaSql){
	                                    $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_weinfo WHERE we_version = '".$dadoVersion."' AND we_platform = '".$dadoPlatform."' AND telefone = '".$AccountIdentifier."';";
	                                    $existente = duplicidadesql($db, $sql);
	                                    if(empty($existente)){
					                		inserirRegistro($db,$sqlInsert);

					                		if($printLogJson){
												$jsonRetorno['6'] = 'OK';
											}
					                	}
					                }

					                if($logGrava){
						                gravalog($FileName, "6");
						                gravalog($FileName, $sqlInsert);
						                gravalog($FileName, $existente . ' - ' . $sqlexistente);
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
					                    //GRAVANDO INFORMAÇÕES DO GRUPO OWNED 
					                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('".$dadoTipoGroup."', '".$pathFile."', '".$dadoThumbnail."', '".$dadoID."', '".$dadoCreation."', '".$dadoSize."', '".$dadoDescription."', '".$dadoSubject."', '".$AccountIdentifier."', ".$ar_id.", '".$dadoPicture."', ".$linh_id.");";
					                    if ($executaSql){
	                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '".$dadoTipoGroup."' AND creation = '".$dadoCreation."' AND id_msg = '".$dadoID."' AND telefone = '".$AccountIdentifier."';";
	                                        $existente = duplicidadesql($db, $sql);
					                    	if(empty($existente)){
					                    		inserirRegistro($db,$sqlInsert);

					                    		if($printLogJson){
													$jsonRetorno['7'] = 'OK';
												}
					                    	}
					                    }

					                    if($logGrava){
						                    gravalog($FileName, "7");
						                    gravalog($FileName, $sqlInsert);
						                    gravalog($FileName, $existente . ' - ' . $sqlexistente);
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
					                    //GRAVANDO INFORMAÇÕES DO GRUPO PARTICIPATING 
					                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_grupoinfo (grouptype, linkedmediafile, thumbnail, id_msg, creation, size, description, subject, telefone, ar_id, imggrupo, linh_id) VALUES ('".$dadoTipoGroup."', '".$pathFile."', '".$dadoThumbnail."', '".$dadoID."', '".$dadoCreation."', '".$dadoSize."', '".$dadoDescription."', '".$dadoSubject."', '".$AccountIdentifier."', ".$ar_id.", '".$dadoPicture."', ".$linh_id.");";
					                    if ($executaSql){
	                                        $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_grupoinfo WHERE grouptype = '".$dadoTipoGroup."' AND creation = '".$dadoCreation."' AND id_msg = '".$dadoID."' AND telefone = '".$AccountIdentifier."';";
	                                        $existente = duplicidadesql($db, $sql);
	                                        if(empty($existente)){
					                    		inserirRegistro($db,$sqlInsert);

								                if($printLogJson){
													$jsonRetorno['8'] = 'OK';
												}
					                    	}
					                    }

					                    if($logGrava){
						                    gravalog($FileName, "8");
						                    gravalog($FileName, $sqlInsert);
						                    gravalog($FileName, $existente . ' - ' . $sqlexistente);
						                }
					                }
					            }
					            
					            if(isset($json->Dados->addressBookInfo)){
					                
					                foreach($json->Dados->addressBookInfo->Symmetriccontacts as $registro){
					                    $dadosymmetricContacts = trim(pg_escape_string($registro));
					                    //GRAVANDO TELEFONES SINCRONA
					                    if(isset($dadosymmetricContacts) && !empty($dadosymmetricContacts)){ 
					                    	$sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('".$dadosymmetricContacts."', 'S', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
					                    	if ($executaSql){
	                                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '".$dadosymmetricContacts."' AND ag_tipo = 'S' AND telefone = '".$AccountIdentifier."';";
	                                            $existente = duplicidadesql($db, $sql);
	                                            if(empty($existente)){
					                    			inserirRegistro($db,$sqlInsert);

								                    if($printLogJson){
														$jsonRetorno['9'] = 'OK';
													}
					                    		}
					                    	}

					                    	if($logGrava){
						                    	gravalog($FileName, "9");
						                    	gravalog($FileName, $sqlInsert);
						                    	gravalog($FileName, $existente . ' - ' . $sqlexistente);
						                    }
					                    }
					                }
					                
					                foreach($json->Dados->addressBookInfo->Asymmetriccontacts as $registro){
					                    $dadoasymmetricContacts = trim(pg_escape_string($registro));
					                    //GRAVANDO TELEFONES ASINCRONA
					                    if(isset($dadoasymmetricContacts) && !empty($dadoasymmetricContacts)){ 
					                    	$sqlInsert = "INSERT INTO leitores.tb_whatszap_agenda (ag_telefone, ag_tipo, telefone, ar_id, linh_id) VALUES ('".$dadoasymmetricContacts."', 'A', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
					                    	if ($executaSql){
	                                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_agenda WHERE ag_telefone = '".$dadoasymmetricContacts."' AND ag_tipo = 'A' AND telefone = '".$AccountIdentifier."';";
	                                            $existente = duplicidadesql($db, $sql);
	                                            if(empty($existente)){
					                    			inserirRegistro($db,$sqlInsert);

					                    			if($printLogJson){
														$jsonRetorno['10'] = 'OK';
													}
					                    		}
					                    	}

					                    	if($logGrava){
						                    	gravalog($FileName, "10");
						                    	gravalog($FileName, $sqlInsert);
						                    	gravalog($FileName, $existente . ' - ' . $sqlexistente);
						                    }
					                    }
					                }
					            }

					            if(isset($json->Dados->smallMediumBusiness)){
					            	//AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
					            	$dadosmallMediumBusiness = trim(pg_escape_string($json->Dados->smallMediumBusiness));		            	
					            }

					            if(isset($json->Dados->ncmecReports)){
					            	//AINDA NÃO IMPLEMENTADO PQ NÃO HOUVE DADOS PARA ANALAISE
					            	$dadoncmecReports = trim(pg_escape_string($json->Dados->ncmecReports));		            	
					            }
							}
							$jsonRetorno['GravaBanco'] = True;
						}else{
							$jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
							$FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a"); 		//CRIANDO ARQUVIVO
							$escreve = fwrite($FileLog, $FileName . ' ' . date('d/m/Y H:i:s') . ' ' . $jsonRetorno['UnidName'] . " Arquivo Existente \n\n");	//ESCREVE NO ARQUIVO LOG
							fclose($FileLog ); //FIM DE LOG
							$jsonRetorno['GravaBanco'] = False;
						}
					}
						 				 
					//ARQUIVOS DO TIPO PRTT 
					if($type == "PRTT"){
						$sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND linh_id = ".$linh_id." AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."';";
						$repetido = selectpadraoconta($db, $sqlexistente);

						if (empty($repetido['ar_id'])){
							$sqlDados = "INSERT INTO leitores.tb_whatszap_arquivo (telefone, ar_dtgerado, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, linh_id) VALUES ('".$AccountIdentifier."', '".$DateRange."', NOW(), '".$FileName."', 0, 1, ".$linh_id.") RETURNING ar_id;";

							if ($executaSql){
	                            $sqlexistente = "SELECT ar_id FROM leitores.tb_whatszap_arquivo WHERE ar_tipo = 0 AND ar_arquivo = '".$FileName."' AND ar_dtgerado = '".$DateRange."';";
	                            $existente = duplicidadesql($db, $sql);
	                            if(empty($existente)){
									$queryArId = inserirRegistroReturning($db,$sqlDados);

									if($printLogJson){
										$jsonRetorno['11'] = 'OK';
									}
								}
							}

							if($logGrava){
								gravalog($FileName, "11");
								gravalog($FileName, $sqlDados);
								gravalog($FileName, $existente . ' - ' . $sqlexistente);
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
							        		$prttSenderPort = null;
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
	                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."' AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."';";
	                                                $existente = duplicidadesql($db, $sql);
	                                                if(empty($existente)){
								        				inserirRegistro($db,$sqlInsert);

								        				if($printLogJson){
															$jsonRetorno['12'] = 'OK';
														}
								        			}
								        		}

								        		if($logGrava){
									        		gravalog($FileName, "12");
									        		gravalog($FileName, $sqlInsert);
									        		gravalog($FileName, $existente . ' - ' . $sqlexistente);
									        	}
								        	}else{
								        		$TipoDirecaoMsg = "Recebeu"; 
								        		$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
								        		if ($executaSql){
	                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."'  AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."';";
	                                                $existente = duplicidadesql($db, $sql);
	                                                if(empty($existente)){
								        				inserirRegistro($db,$sqlInsert);

								        				if($printLogJson){
															$jsonRetorno['13'] = 'OK';
														}
								        			}
								        		}
								        		if($logGrava){
									        		gravalog($FileName, "13");
									        		gravalog($FileName, $sqlInsert);
									        		gravalog($FileName, $existente . ' - ' . $sqlexistente);
									        	}
								        	}	
								        }else{
								        	//VERIFICAÇÃO PARA INSERIR AS TROCAS DE MENSAGENS GROUP
								        	if($prttSender == $AccountIdentifier){
								        		$TipoDirecaoMsg = "Enviou"; 
								        		$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttSender."', '".$prttRecipients."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
								        		if ($executaSql){
	                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."' AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."';";
	                                                $existente = duplicidadesql($db, $sql);
	                                                if(empty($existente)){
								        				inserirRegistro($db,$sqlInsert);

								        				if($printLogJson){
															$jsonRetorno['14'] = 'OK';
														}
								        			}
								        		}

								        		if($logGrava){
									        		gravalog($FileName, "14");
									        		gravalog($FileName, $sqlInsert);
									        		gravalog($FileName, $existente . ' - ' . $sqlexistente);
									        	}
								        	}else{
								        		$TipoDirecaoMsg = "Recebeu"; 
								        		$sqlInsert = "INSERT INTO leitores.tb_whatszap_index_zapcontatos_new (datahora, messageid, sentido, alvo, interlocutor, groupid, senderip, senderport, senderdevice, messagesize, typemsg, messagestyle, telefone, ar_id, linh_id) VALUES ('".$prttTimestamp."', '".$prttMessageId."', '".$TipoDirecaoMsg."', '".$prttRecipients."', '".$prttSender."', '".$prttGroupId."', '".$prttSenderIp."', ".$prttSenderPort.", '".$prttSenderDevice."', ".$prttMessageSize.", '".$prttType."', '".$prttMessageStyle."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.");";
								        		if ($executaSql){
	                                                $sqlexistente = "SELECT indn_id FROM leitores.tb_whatszap_index_zapcontatos_new WHERE messageid = '".$prttMessageId."'  AND datahora = '".$prttTimestamp."' AND telefone = '".$AccountIdentifier."';";
	                                                $existente = duplicidadesql($db, $sql);
	                                                if(empty($existente)){
								        				inserirRegistro($db,$sqlInsert);

								        				if($printLogJson){
															$jsonRetorno['15'] = 'OK';
														}
								        			}
								        		}
								        		if($logGrava){
									        		gravalog($FileName, "15");
									        		gravalog($FileName, $sqlInsert);
									        		gravalog($FileName, $existente . ' - ' . $sqlexistente);
									        	}
								        	}
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
							            if(isset($registro->callEvents)){
							                foreach($registro->callEvents as $subregistro){
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
							                		$prttEsolPort = null;
							                	}
							                	if(isset($subregistro->mediaType)){
							                		$prttEmediaType = trim(pg_escape_string($subregistro->mediaType));
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
								                			//INSERT DE CHAMADAS TROCADAS EM ALVO/INTERLOCUTOR 
										                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUES ( '".$prttcallID."', '".$prttcallCreator."', '".$prttEtype."', '".$prttEtimestamp."', '".$prttEsolicitante."', '".$prttEatendente."', '".$prttEsolIP."', '".$prttEsolPort."', '".$prttEmediaType."', '".$prttPhoneNumber."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.", '".$TipoDirecaoCall."');";
										                    if ($executaSql){
	                                                            $sqlexistente = "SELECT cal_id FROM leitores.tb_whatszap_call_log WHERE call_id = '".$prttcallID."' AND call_creator = '".$prttcallCreator."' AND call_timestamp = '".$prttEtimestamp."' AND call_from = '".$prttEsolicitante."' AND call_to = '".$prttEatendente."' AND call_from_ip = '".$prttEsolIP."' AND call_media_type = '".$prttEmediaType."' AND call_phone_number = '".$prttPhoneNumber."' AND telefone = '".$AccountIdentifier."';";
	                                                            $existente = duplicidadesql($db, $sql);
	                                                            if(empty($existente)){
										                    		inserirRegistro($db,$sqlInsert);

										                    		if($printLogJson){
																		$jsonRetorno['16'] = 'OK';
																	}
										                    	}
										                    }

										                    if($logGrava){
											                    gravalog($FileName, "16");
									                    		gravalog($FileName, $sqlInsert);
									                    		gravalog($FileName, $existente . ' - ' . $sqlexistente);
									                    	}
									                	}
									                }
							                	}else{					                	
								                    //INSERT DE CHAMADAS TROCADAS EM ALVO/INTERLOCUTOR 
								                    $sqlInsert = "INSERT INTO leitores.tb_whatszap_call_log (call_id, call_creator, call_type, call_timestamp, call_from, call_to, call_from_ip, call_from_port, call_media_type, call_phone_number, telefone, ar_id, linh_id, sentido) VALUSE ('".$prttcallID."', '".$prttcallCreator."', '".$prttEtype."', '".$prttEtimestamp."', '".$prttEsolicitante."', '".$prttEatendente."', '".$prttEsolIP."', '".$prttEsolPort."', '".$prttEmediaType."', '".$prttPhoneNumber."', '".$AccountIdentifier."', ".$ar_id.", ".$linh_id.", '".$TipoDirecaoCall."');";
								                    if ($executaSql){
	                                                    $sqlexistente = "SELECT cal_id FROM leitores.tb_whatszap_call_log WHERE call_id = '".$prttcallID."' AND call_creator = '".$prttcallCreator."' AND call_timestamp = '".$prttEtimestamp."' AND call_from = '".$prttEsolicitante."' AND call_to = '".$prttEatendente."' AND call_from_ip = '".$prttEsolIP."' AND call_media_type = '".$prttEmediaType."' AND call_phone_number = '".$prttPhoneNumber."' AND telefone = '".$AccountIdentifier."';";
	                                                    $existente = duplicidadesql($db, $sql);
	                                                    if(empty($existente)){
								                    		inserirRegistro($db,$sqlInsert);

								                    		if($printLogJson){
																$jsonRetorno['17'] = 'OK';
															}
								                    	}
								                    }

								                    if($logGrava){
									                    gravalog($FileName, "17");
									                    gravalog($FileName, $sqlInsert);
									                    gravalog($FileName, $existente . ' - ' . $sqlexistente);
									                }
							                	}
							                }
							            }
							        }
							    }
							    
							    /*if(isset($json->Prtt->fileContent)){ 

							        $sqlUpdate = "UPDATE leitores.tb_whatszap_arquivo SET ar_json = '".pg_escape_string($json->Prtt->fileContent)."' WHERE ar_id = ".$ar_id;

							        if ($executaSql){
										alterarRegistro($db,$sqlUpdate);
										$jsonRetorno['18'] = 'OK';
							        }
							        if($logGrava){
										gravalog($FileName, "18");
								        gravalog($FileName, $sqlUpdate);
								        gravalog($FileName, $existente . ' - ' . $sqlexistente);
								    }
							    }*/
							}
						}else{
							$jsonRetorno['Repetido'] = "Arquivo Existente " . $FileName;
							$FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a"); 		//CRIANDO ARQUVIVO
							$escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \nArquivo Existente \n\n");	//ESCREVE NO ARQUIVO LOG
							fclose($FileLog ); //FIM DE LOG
							$jsonRetorno['GravaBanco'] = False;
						}
					}						
				}else{
					$FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a"); 		//CRIANDO ARQUVIVO
					$escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] . " \n" . $sqllinh_id ." \nLinha Não Localizada \n\n");	//ESCREVE NO ARQUIVO LOG
					fclose($FileLog ); //FIM DE LOG
					$jsonRetorno['GravaBanco'] = False;
				}				
			}else{
				$FileLog = fopen("ArquivoLogZipNaoProcessados.txt", "a"); 		//CRIANDO ARQUVIVO
				$escreve = fwrite($FileLog, $FileName . " \n" . date('d/m/Y H:i:s') . " \n" . $jsonRetorno['UnidName'] ." \nErro de Conta ou Unidade \n\n");	//ESCREVE NO ARQUIVO LOG
				fclose($FileLog ); //FIM DE LOG
				$jsonRetorno['GravaBanco'] = False;
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
		        		$prttSenderPort = null;
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
		            if(isset($registro->callEvents)){
		            	
		                foreach($registro->callEvents as $subregistro){
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
		                		$prttEsolPort = null;
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