<?php
if(!isset($_SESSION)){
	session_start();
}

define('DB_HOST', '10.115.136.217');
define('DB_NAME','andromeda');
define('DB_USER','');
define('DB_PASS','');
define('DB_PORT','5432');	
define('DB_SGBD','pgsql');

class ConexaoPDO{
	public $con;	
	public function __construct(){
		try{
			$this->con=new PDO(DB_SGBD.':host='.DB_HOST.' dbname='.DB_NAME.' port='.DB_PORT.' user='.DB_USER.' password='.DB_PASS);
			$this->con->setAttribute(PDO::ATTR_PERSISTENT, false);//
			$this->con->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
			$this->con->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ);			
		}catch(PDOException $e){
			echo 'Erro: '. $e->getMessage() .'<br /> Código: '.$e->getCode() .'<br /> Arquivo: '.$e->getFile() .'<br /> Linha: '.$e->getLine();
		}
	}
}

//CHAMA A CONEXÃO
try{
	$db = new ConexaoPDO();
}catch(PDOException $e){
	throw new PDOException($e);
}

//--------------------------------SELECT UMA LINHA REGISTRO DO SISTEMA-------------------------------------------//
function selectpadraoumalinha($db,$sql){
	try{
		$verificar=$db->con->prepare($sql);
		$verificar->execute();
		if(isset($verificar)){				
			$resultado=$verificar->fetch(PDO::FETCH_ASSOC);
			$db=null;
			return($resultado);
		}	
	}catch(PDOException $error){
		// Adicionado 08/08/17 - Glauco
		$fp = fopen("Erro-SQL-Linha307.log", "a"); //CRIANDO ARQUVIVO
		$escreve = fwrite($fp ,"\n\n SQL UMA LINHA ERRO - : ".$sql);//ESCREVE NO ARQUIVO LOG
		fclose($fp); //FECHA ARQUIVO
		
		return($error->getMessage());
	}
}

//--------------------------------INSERIR REGISTRO DO SISTEMA-------------------------------------------//
function inserirRegistro($db,$sql){		
	try{	
		$inserir=$db->con->prepare($sql);
		$inserir->execute();
		if($inserir==1){
			return 1;
		}else{
			return 0;
		}
		$db=null;
	}catch(PDOException $error){
		return($error->getMessage());
	}
}
	
	
//--------------------------------INSERIR REGISTRO RETURNING ID-------------------------------------------//
function inserirRegistroReturning($db,$sql){		
	try{	
		$inserir=$db->con->prepare($sql);
		$inserir->execute();
		if($inserir>0){
			$ultimo_id=$inserir->fetch(PDO::FETCH_ASSOC);
			return($ultimo_id);
		}
		else{
			return(0);
		}
		$db=null;
	}catch(PDOException $error){
		return($error->getMessage());
	}
}

//--------------------------------ALTERAR REGISTRO DO SISTEMA-------------------------------------------//
function alterarRegistro($db,$sql){	
	try{	
		$alterar=$db->con->prepare($sql);
		$alterar->execute();
		if($alterar==1){
			return(1);
		}
		else{
			return($alterar);
		}
		$db=null;
	}catch(PDOException $error){
	
		// Adicionado 09/08/17 - Glauco
		$fp = fopen("Erro-Alterar-Linha195.log", "a"); //CRIANDO ARQUVIVO
		$escreve = fwrite($fp ,"\n\n SQL Verifica Permissão ERRO - : ".$sql);//ESCREVE NO ARQUIVO LOG
		fclose($fp); //FECHA ARQUIVO
		return($error->getMessage());
	}
}
//--------------------------------EXCLUIR REGISTRO DO SISTEMA-------------------------------------------//
function excluirRegistro($db,$sql){		
	try{	
		$exclui=$db->con->prepare($sql);
		$exclui->execute();
		if($exclui==1){
			return(1);
		}else{
			return(0);
		}
		$db=null;
	}catch(PDOException $error){
		return($error->getMessage());
	}
}
//--------------------------------FUNCAO SELECT PADRAO----------------------------------//
function selectpadrao($db,$sql){
	try{
		$verificar=$db->con->prepare($sql);
		$verificar->execute();
		$resultado=$verificar->fetchAll(PDO::FETCH_ASSOC);
		$db=null;
		return($resultado);
	}catch(PDOException $error){
		return($error->getMessage());
	}
}
//--------------------------------RETORNA O IP----------------------------------//
function retornaIpReal() {  //Retorna o IP que esta acessando o Site em Tempo Real
	 if (!empty($_SERVER['HTTP_CLIENT_IP'])){  //se possível, obtém o endereço ip da máquina do cliente
		$ip=$_SERVER['HTTP_CLIENT_IP'];
	 }elseif (!empty($_SERVER['HTTP_X_FORWARDED_FOR'])){  //verifica se o ip está passando pelo proxy
		$ip=$_SERVER['HTTP_X_FORWARDED_FOR'];
	 }else{
		$ip=$_SERVER['REMOTE_ADDR'];
	 }
	 return $ip;
}
//--------------------------------NAVEGADOR SENDO UTILIZADO----------------------------------//
function tipoNavegador (){
	$useragent = $_SERVER['HTTP_USER_AGENT'];

	if (preg_match('|MSIE ([0-9].[0-9]{1,2})|',$useragent,$matched)) {
		$browser_version=$matched[1];
		$browser = 'IE';
	  } elseif (preg_match( '|Opera/([0-9].[0-9]{1,2})|',$useragent,$matched)) {
		$browser_version=$matched[1];
		$browser = 'Opera';
	  } elseif(preg_match('|Firefox/([0-9\.]+)|',$useragent,$matched)) {
		$browser_version=$matched[1];
		$browser = 'Firefox';
	  } elseif(preg_match('|Chrome/([0-9\.]+)|',$useragent,$matched)) {
		$browser_version=$matched[1];
		$browser = 'Chrome';
	  } elseif(preg_match('|Safari/([0-9\.]+)|',$useragent,$matched)) {
		$browser_version=$matched[1];
		$browser = 'Safari';
	  } else {
		// browser not recognized!
		$browser_version = 0;
		$browser= 'Desconhecido';
	}
	$navegador = $browser." ".$browser_version; //Navegador Utilizado
	return($browser);
}
//--------------------------------NOME REAL----------------------------------//
function NomeMaquinaRem(){
	$Nome = gethostbyaddr(retornaIpReal());
	return $Nome;
}
//--------------------------------SELEC PADRAO CONTA----------------------------------//
function selectpadraoconta($db,$sql){

    try{
        $verificar=$db->con->prepare($sql);
        $verificar->execute();
        $resultado=$verificar->rowCount();
        $db=null;
        return($resultado);
    }catch(PDOException $error){
        return($error->getMessage());
    }
}