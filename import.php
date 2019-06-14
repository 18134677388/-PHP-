<?php
	include_once("/var/www/reporter/system/adodb/adodb-exceptions.inc.php");
	include_once("/var/www/reporter/system/adodb/adodb.inc.php");
	
	$fname = trim($argv[1]);									//导出文件的脚本
	$importpath = $argv[2];								//文件存储路径
	$xhcolumn = empty($argv[3])?1:$argv[3];
	$xmcolumn = empty($argv[4])?1:$argv[4];
	if(empty($fname) || !file_exists($fname)) {
		exit("参数一：".$fname."; 没有找到名单文件!");
	} 
	if(empty($importpath)) {
		exit("参数二：".$importpath."; 没有指定文件生成的目录!");
	} 
	if(!file_exists($importpath)) {
		system("mkdir -p $importpath");
	}
	$arr = parserFname($fname);
	$md5fname = $importpath . "/" . md5_file($fname)."_pos.log";
	$startserno = getStartSerno($md5fname);
	
	$con = @mysql_connect("localhost","root","123456");
	foreach($arr as $t) {
		if($t[1] < $startserno) {
			continue;
		}
		//$t[1] = "172.16.162.141";
		setCsv($t);
		//break;
		file_put_contents($md5fname,$t[1]);
	}
	mysql_close($con);
	
	function getBehaviorDb($time) {
		$dbarr = array();
		$sql = "SELECT `TABLE_NAME` FROM `information_schema`.`TABLES`";
		$sql .= " WHERE `TABLE_SCHEMA` = 'log-$time'";
		$sql .= " AND (`TABLE_NAME` LIKE 'log-http_url-%'";
		$sql .= " OR `TABLE_NAME` LIKE 'log-behavior-%')";
		//echo $sql ; 
		$result = mysql_query($sql);
		while ($row = @mysql_fetch_array($result,MYSQL_ASSOC)) {
			if (strrpos ( $row ["TABLE_NAME"], "log-http_url" ) === 0) {
				$dbarr["http_url"][] = $row["TABLE_NAME"];
			} else {
				$dbarr["behavior"][] = $row["TABLE_NAME"];
			}
		}
		return $dbarr;
	}
	function setCsv($info) {
		global $importpath;
		echo "学号:".$info[1]." 序号:".$info[0]." 姓名:".$info[2]."，开始导上网日志。。。。\r\n";
		$csvPath = $importpath."/".$info[1];
		if(file_exists($csvPath)) {
			exec("rm -rf $csvPath");
		}
		mkdir($csvPath);
		for($m = 5; $m < 13; $m++) {
			for($n= 1; $n < 32; $n++) {
				$time = sprintf("2017-%02d-%02d",$m,$n);
				echo "导出上网日日期：".$time.";".$csvPath."/behavior/".$time."\r\n";
				setBehaviorFile($csvPath."/behavior",$time,$info);
				//echo "导出用户上网时长：".$time.";".$csvPath."/online/".$time."\r\n";
				//setOnlineFile($csvPath."/online",$time,$info);
			}
		}
		echo "学号:".$info[1]." 序号:".$info[0]." 姓名:".$info[2]."，导出完成。。。。\r\n";
	}
	function setOnlineFile($csvPath,$time,$info) {
		if(!file_exists($csvPath)) {
			mkdir($csvPath);
		}
		$query = "select M2.name_zh_CN as sname,M3.name_zh_CN as gname,user,onlinetot from (";
		$query .= "select service,servicetype,user,sum(online) as onlinetot from `stat-daily-$time`.`online` ";
		$query .= " where user='".$info[1]."' group by service,servicetype,user";
		$query .= ") M1 left join `sysparam`.`service` M2 on M1.service=M2.id";
		$query .= " left join `sysparam`.`service_group` M3 on M1.servicetype=M3.id";
		echo $query."\r\n";
		$file = fopen($csvPath."/".$time.".csv","w");
		$result = mysql_query($query);
		$f = false;
		while ($row = @mysql_fetch_array($result,MYSQL_ASSOC)) {
			$csvArr = array($row["user"],$row ["sname"],$row["gname"],ShowOnline($row["onlinetot"]));
			fputcsv($file,$csvArr);
			$f = true;
		}
		fclose($file);
		if($f === false) {
			@unlink($csvPath."/".$time.".csv");
		}
	}
	function ShowOnline($online){
		$sec = $online;
		$day = floor($sec/86400);
		$sec -= $day*86400;
		$hour = floor($sec/3600);
		$sec -= $hour*3600;
		$min = floor($sec/60);
		$sec -= $min*60;
		if($day)
			$str .= $day."天";
		if($hour)
			$str .= $hour."时";
		if($min)
			$str .= $min."分";
		if($sec)
			$str .= $sec."秒";
		return $str;
	}
	function setBehaviorFile($csvPath,$time,$info) {
		$appObj = new RecordType ();
		$appArr = $appObj->getApp ();
		
		if(!file_exists($csvPath)) {
			mkdir($csvPath);
		}
		$dbarr = getBehaviorDb($time);
		//var_dump($dbarr);
		if(count($dbarr) <= 0) {
			return;
		}
		$sql1 = "";
		foreach($dbarr["behavior"] as $k1) {
			if(!empty($sql1)) {
				$sql1 .= " union all ";
			}
			$sql1 .= " (select `user`,`app`,`serv1`,`serv2`,`time`,`crc` from `log-".$time."`.`".$k1."` where user='".$info[1]."') ";
		}
		$sql2 = "";
		foreach($dbarr["http_url"] as $k1) {
			if(!empty($sql2)) {
				$sql2 .= " union all ";
			}
			$sql2 .= " (select `http_url`,`crc` from `log-".$time."`.`".$k1."` where user='".$info[1]."') ";
		}
		if(!empty($sql1)) {
			$query =  "select M1.`user`,M1.`app`,M1.`serv1`,M1.`serv2`,M1.`time`";
			if(!empty($sql2)) {
				$query .=  " ,M2.http_url";
			}
			$query .=  " from ($sql1) M1";
			
		}
		if(!empty($sql2)) {
			$query .=  " left join ($sql2) M2 on M1.crc=M2.crc";
		}
		echo $query."\r\n";
		$f = false;
		$file = fopen($csvPath."/".$time.".csv","w");
		$result = mysql_query($query);
		while ($row = @mysql_fetch_array($result,MYSQL_ASSOC)) {
			if($row["app"] == "1") {
				if($row ["serv1"] == "2")
					$row["app"] = "11";
				if($row ["serv1"] == "1")
					$row["serv1"] = $row["serv2"];
			}
			if ($row ["app"] == "2") {
				$xwstr = $appObj->getAppOther ( $row ["app"], $row ["serv2"] );
			} else {
				$app1_list = $appObj->getApp1 ($row["app"]);
				$app2_list = $appObj->getApp2 ( $row ["app"], $row ["serv1"] );
				$xwstr = $app1_list [$row ["serv1"]];
				if ($app2_list != "")
					$xwstr .= "-" . $app2_list [$row ["serv2"]];
			}
			$csvArr = array($row["user"],$appArr [$row ["app"]],$xwstr,$row["http_url"],date('Y-m-d H:i:s',$row["time"]));
			fputcsv($file,$csvArr);
			$f = true;
		}
		fclose($file);
		if($f === false) {
			@unlink($csvPath."/".$time.".csv");
		}
	}
	//获取文件的开始序列号
	function getStartSerno($md5fname) {
		if(file_exists($md5fname)) {
			return file_get_contents($md5fname);
		}
		return 0;
	}
	/**
	*解析数据文件
	**/
	function parserFname($fname) {
		global $xhcolumn;
		global $xmcolumn;
		$ret = array();
		$arr = file($fname);
		if(empty($arr)) {
			exit("名单文件".$fname."没有数据内容!");
		}
		foreach($arr as $v) {
			$tmp = explode("	",$v);
			foreach($tmp as &$v1) {
				$v1 = trim($v1);
			}
			$serno = $tmp[0];
			$xm = $tmp[$xmcolumn];
			$xh = $tmp[$xhcolumn];
			if(empty($serno) || empty($xm) || empty($xh)) {
				continue;
			}
			$ret[] = array($serno,$xh,$xm);
		}
		return $ret;
	}
	
	abstract class BaseSql 
	{
		protected $dbserver = array();
		protected $conn = NULL;
		protected $rs = NULL;
		
		public function __construct()
		{	
			$this->dbserver["dbtype"]= "mysql";
			$this->dbserver["server"]= "localhost:13361";
			$this->dbserver["database"]= "";
			$this->dbserver["user"]= "root";
			$this->dbserver["password"]="123456";
			$this->GetDatabaseConn();
		}
		
		public function __destroy()
		{
			if ( $this->conn )
				$this->conn->Close();
		} 

		private function ThrowErrMsg($errMsg="",$dataListType)
		{
			echo $errMsg."\r\n";
		}

		
		public function SetParams($dbserver)
		{
			$this->dbserver = $dbserver;
			$this->GetDatabaseConn();
		
		}
		protected function GetDatabaseConn()
		{
			$database= $this->dbserver["database"];

			$this->conn = &ADONewConnection($this->dbserver["dbtype"]);
			
			$this->conn->debug = $this->dbserver["debug"];
			$this->conn->SetFetchMode(ADODB_FETCH_ASSOC);
			
			try
			{
				$result = $this->conn->Connect($this->dbserver["server"], $this->dbserver["user"], $this->dbserver["password"], $database);
				$this->conn->Execute("SET NAMES 'UTF8'");
			}
			catch (exception $e) 
			{
				//print $e->getMessage(); 
				return $this->ThrowErrMsg(_("db no answer"),0);
			}

			return $result;
		}
		// 用户查询数据list
		public function  GetQueryData($query_string)
		{
			if ( !$this->conn || $query_string == "" )
				return NULL;
			try 
			{ 
				$rs = $this->conn->GetAll($query_string);
			} 
			catch (exception $e) 
			{ 
				//$logger =  $e->getMessage(); 
				//print $logger;
				return NULL;
			}
			
			return $rs;
		}
		public function GetQueryRS($query_string)
		{
			if ( !$this->conn || $query_string == "" )
				return NULL;
			try 
			{ 
				$rs = &$this->conn->Execute($query_string);
			} 
			catch (exception $e) 
			{ 
				return NULL;
			}  
			$this->rs = $rs;
			return $rs;
		}
		public function GetRS()
		{
			return $this->rs;
		}
		
		public function  GetQueryDataByNoDie($query_string)
		{
			if ( !$this->conn )
				return NULL;
			try 
			{ 
				$rs = $this->conn->Execute($query_string);
			} 
			catch (exception $e) 
			{ 
				return NULL;
			}  
				
			return $rs->GetRows();
		}
		// delete,drop,update等数据操作
		public function GetQueryResult($query_string)
		{
		
			if ( !$this->conn || (is_string($query_string)&&$query_string == "") )
				return false;
			try 
			{ 
				if (is_array($query_string)) {
					foreach ($query_string as $value){
						$rs = $this->conn->Execute($value);
					}
				}else {
					$rs = $this->conn->Execute($query_string);
				}
				
			} 		
			catch (exception $e) 
			{ 
				return false;
			}  
			return $this->conn->Affected_Rows() > 0  ? true : false;
		}
		
		// 查询统计数据 select count(*) from tb
		public function GetQueryScalar($query_string)
		{
		
			if ( !$this->conn || $query_string == "" )
				return false;
			try
			{
				$rs = $this->conn->getOne($query_string);
			}
			catch (exception $e)
			{
				return false;
			}
			return $rs;
		}
		
		public function GetQueryDataLimit($query_string,$size,$offset){
		
			if ( !$this->conn || $query_string == "" )
				return NULL;
			try
			{
				$rs = $this->conn->SelectLimit($query_string,$size,$offset);
			}
			catch (exception $e)
			{
				return NULL;
			}
			//return adodb_getall($rs);
		
		}
	}
	class RecordType extends BaseSql {
		private $httptype = "";
		private $httpupload = "";
		private $searchgroup = "";
		private $searchlist = "";
		private $forumgroup = "";
		private $forumlist = "";
		private $loginlist = "";
		private $logingroup = "";
		public function getServListByapp($app, $serv1) {
			if ($app == '2') {
				$ret = $this->getSearchList ();
			} else if ($app == "5") {
				$ret = $this->GetLoginList ();
			} else if ($app == "8") {
				$ret = $this->getFormList ();
			}
			return $ret [$serv1];
		}
		public function getTableNameApp() {
			return array (
					"1" => array("http_title","http_url","http_post"),
					"2" => "http_search",
					"4" => "file_transfer",
					"5" => "app_login",
					"6" => "mail",
					"7" => "app_chat",
					"8" => "http_bbs",
					"9" => "telnet_command",
					"10" => "drop_behavior"        
			);
		}
		public function getApp() {
			return array (
					"1" => "访问网站",
					"2" => "网页搜索",
					"6" => "邮件收发",
					"7" => "IM聊天",
					"8" => "论坛微博",
					"5" => "帐号登录",
					"4" => "外发文件",
					"11"=>"外发信息",
					"9" => "telnet 命令",
					"10"=> "阻断记录" 
			);
		}
		public function getApp1($app) {
			if($app == "1") {
				$this->getHttpType ();
				return $this->httptype;
			}
			if ($app == "2") {
				$this->getSearchGroup ();
				return $this->searchgroup;
			}
			if ($app == "3")
				return array (
						"1" => "IM登陆",
						"2" => "退出"  
				);
			if ($app == "4")
				return array (
						"1" => "HTTP传输",
						"2" => "IM传输",
						"3" => "FTP传输",
						"4" => "邮件收发" 
				);
			if ($app == "5") {
				$this->GetLoginGroupName ();
				return $this->logingroup;
				/*
				 * return array ( "1" => "HTTP登陆", "2" => "IM登陆", "3" => "FTP登陆" );
				 */
			}
			if ($app == "6")
				return array (
						"2" => "WEB邮件发送",
						"3" => "POP3收发",
						"4" => "smtp收发",
						"5" => "imap收发",
				);
			if ($app == "7")
				return array (
						"1" => "QQ",
						"2" => "MSN",
						"3" => "Yahoo",
						"4" => "SKYPE",
						"5" => "GTALK",
						"6" => "阿里巴巴",
						"7" => "其他",
				);
			if ($app == "8") {
				$this->getForumGroup ();
				return $this->forumgroup;
			}
			if ($app == "10")
				return array (
						"1" => "防火墙阻断",
						"2" => "流量阻断",
						"3" => "行为阻断",
				);
			return "";
		}
		public function getApp2($app1, $app2) {
			
			if ($app1 == "2") {
				$this->getSearchList ();
				return $this->searchlist [$app2];
			}
			if ($app1 == "4") {
				if ($app2 == "1") {
					$this->getHttpUpload ();
					return $this->httpupload;
				}
				if ($app2 == "2") {
					return array (
							"1" => "QQ",
							"2" => "MSN",
							"3" => "Yahoo",
							"4" => "SKYPE",
							"5" => "GTALK",
							"6" => "阿里巴巴",
							"7" => "其他",
							"9" => "微信",
					);
				}
				if ($app2 == "3") {
					return array (
							"1" => "下载",
							"2" => "上传" 
					);
				}
				if ($app2 == "4") {
					return array (
						"2" => "WEB邮件发送",
						"3" => "POP3收发",
						"4" => "smtp收发",
						"5" => "imap收发",
					);
				}
			}
			if ($app1 == "5") {
				$this->GetLoginList ();
				return $this->loginlist [$app2];
			}
			if ($app1 == "8") {
				$this->getFormList ();
				return $this->forumlist [$app2];
			}
			return "";
		}
		public function GetLoginGroupName() {
			if ($this->logingroup == "") {
				$query = "SELECT id,name_zh_CN as name FROM sysparam.http_login_group ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->logingroup [$result [$i] ["id"]] = $result [$i] ["name"];
					}
				}
			}
			return $this->logingroup;
		}
		public function GetLoginList() {
			if ($this->loginlist == "") {
				$query = "SELECT id,group_id,name_zh_CN  as name FROM sysparam.http_login ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++)
						$this->loginlist [$result [$i] ["group_id"]] [$result [$i] ["id"]] = $result [$i] ["name"];
				}
			}
			return $this->loginlist;
		}
		public function getForumGroup() {
			if ($this->forumgroup == "") {
				$query = "SELECT id,name_zh_CN  as name FROM sysparam.http_bbs_group ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->forumgroup [$result [$i] ["id"]] = $result [$i] ["name"];
					}
				}
				//$this->forumgroup [- 1] = _ ( "other" );
			}
			return $this->forumgroup;
		}
		public function getFormList() {
			if ($this->forumlist == "") {
				$query = "SELECT id,group_id,name_zh_CN  as name FROM sysparam.http_bbs ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++)
						$this->forumlist [$result [$i] ["group_id"]] [$result [$i] ["id"]] = $result [$i] ["name"];
				}
			}
			return $this->forumlist;
		}
		public function getAppOther($app, $app2) {
			if ($app == "5") {
				$this->GetLoginGroupName ();
				$this->GetLoginList ();
				if (is_array ( $this->loginlist )) {
					foreach ( $this->loginlist as $key => $search ) {
						if (is_array ( $search )) {
							foreach ( $search as $key1 => $val ) {
								if ($key1 == $app2) {
									return $this->logingroup [$key] . "-" . $val;
								}
							}
						}
					}
				}
			}
			if ($app == "2") {
				$this->getSearchGroup ();
				$this->getSearchList ();
				if (is_array ( $this->searchlist )) {
					foreach ( $this->searchlist as $key => $search ) {
						if (is_array ( $search )) {
							foreach ( $search as $key1 => $val ) {
								if ($key1 == $app2) {
									return $this->searchgroup [$key] . "-" . $val;
								}
							}
						}
					}
				}
			}
			if ($app == "8") {
				$this->getForumGroup ();
				$this->getFormList ();
				if (is_array ( $this->forumlist )) {
					foreach ( $this->forumlist as $key => $search ) {
						if (is_array ( $search )) {
							foreach ( $search as $key1 => $val ) {
								if ($key1 == $app2) {
									return $this->forumgroup [$key] . "-" . $val;
								}
							}
						}
					}
				}
			}
		}
		private function getSearchList() {
			if ($this->searchlist == "") {
				$query = "SELECT id,group_id,name_zh_CN  as name FROM sysparam.http_search ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++)
						$this->searchlist [$result [$i] ["group_id"]] [$result [$i] ["id"]] = $result [$i] ["name"];
				}
				//$this->searchlist [0] = _ ( "other" );
			}
			return $this->searchlist;
		}
		private function getSearchGroup() {
			if ($this->searchgroup == "") {
				$query = "SELECT id,name_zh_CN  as name FROM sysparam.http_search_group ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->searchgroup [$result [$i] ["id"]] = $result [$i] ["name"];
					}
				}
				//$this->searchgroup [0] = _ ( "other" );
			}
			return $this->searchgroup;
		}
		private function getHttpUpload() {
			if ($this->httpupload == "") {
				$list = array ();
				$query = "SELECT id,name_zh_CN  as name FROM sysparam.http_upload ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->httpupload [$result [$i] ["id"]] = $result [$i] ["name"];
					}
				}
			}
			return $this->httpupload;
		}
		private function getHttpType() {
			if ($this->httptype == "") {
				$this->httptype = array ();
				$query = "SELECT id,name_zh_CN  as name FROM sysparam.http_category ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->httptype [$result [$i] ["id"]] = $result [$i] ["name"];
					}
				}
			}
			return $this->httptype;
		}
		public function getServiceName() {
			if ($this->servicename == "") {
				$this->servicename = array ();
				$query = "SELECT id,group_id,name_zh_CN  as name FROM sysparam.service ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$this->servicename ["name"] [$result [$i] ["id"]] = $result [$i] ["name"];
						$this->servicename ["gid"] [$result [$i] ["id"]] = $result [$i] ["group_id"];
					}
				}
			}
			return $this->servicename;
		}
		public function getServiceGroupName() {
			if ($this->servicegroupname == "") {
				$this->servicegroupname = array ();
				$query = "SELECT id,pid,name_zh_CN  as name FROM sysparam.service_group ORDER BY id";
				$result = $this->GetQueryData ( $query );
				if ($result && count ( $result ) > 0) {
					for($i = 0; $i < count ( $result ); $i ++) {
						$pName = "";
						if($result[$i]["pid"] > 0) {
							for($j=0;$j<count($result);$j++)
							{
								if($result[$j]["id"] == $result[$i]["pid"]) {
									$pName = $result[$j]["name"] . "/";
									break;
								}
							}
						}
						$this->servicegroupname [$result [$i] ["id"]] = $pName.$result [$i] ["name"];
					}
				}
			}
			return $this->servicegroupname;
		}
	}


?>
