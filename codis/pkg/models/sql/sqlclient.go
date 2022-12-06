// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package sqlclient

import (
	"fmt"
	"strings"
	"sync"
	"database/sql"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/coopernurse/gorp"
	_ "github.com/go-sql-driver/mysql"
)

const (
	ReadNode    = "read_node"
	CreateNode  = "create_node"
	DeleteNode  = "delete_node"
	UpdateNode  = "update_node"
)

var ErrClosedClient = errors.New("use of closed mysql client")
var ErrNotSupported = errors.New("not supported")
var table = "codis_dashboard"

type Client struct {
	sync.Mutex
	dbmap *gorp.DbMap
	
	closed bool
}

func New(addrlist string, username string, password string, database string) (*Client, error) {
	if (addrlist == "" || password == "" || username == "" || database == "") {
		log.Warnf("initDb: invalid params")
		return nil, errors.New("invalid params")
	}

	dbmap, err := initDb(addrlist, username, password, database)
	if err != nil {
		return nil, err
	}
	return &Client{dbmap: dbmap, closed: false}, nil
}

func initDb(addr string, user string , pswd string , database string) (*gorp.DbMap, error) {
	if (addr == "" || user == "" || pswd == "" || database == "") {
		log.Warnf("initDb: invalid params")
		return nil, errors.New("invalid params")
	}

	var netAddr = fmt.Sprintf("%s(%s)", "tcp", addr)
	var dsn = fmt.Sprintf("%s:%s@%s/%s", user, pswd, netAddr, database)
	//db, err := sql.Open("mysql", user + ":" + pswd + "@tcp(" + addr + ")/" + database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.ErrorErrorf(err, "faile to open database")
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.ErrorErrorf(err, "faile to connect database")
		return nil, err
	}

	//create tables to store user information
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}
	log.Warnf("dbmap open success")
	return dbmap, nil
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	if c.dbmap != nil {
		c.dbmap.Db.Close()
	}
	return nil
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("sqlclient create node %s", path)

	_, err := c.exec(path, data, CreateNode)
	if err != nil {
		log.Debugf("sqlclient create node %s failed: %s", path, err)
		return err
	}
	log.Debugf("sqlclient create OK")
	return nil
}

func (c *Client) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("sqlclient create node %s", path)

	_, err := c.exec(path, data, UpdateNode)
	if err != nil {
		log.Debugf("sqlclient update node %s failed: %s", path, err)
		return err
	}
	log.Debugf("sqlclient create OK")
	return nil
}

func (c *Client) Delete(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("sqlclient delete node %s", path)

	_, err := c.exec(path, nil, DeleteNode)
	if err != nil {
		log.Debugf("sqlclient create node %s failed: %s", path, err)
		return err
	}
	log.Debugf("sqlclient create OK")
	return nil
}

func (c *Client) Read(path string, must bool) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	log.Debugf("sqlclient read node %s", path)

	nodeJson, err := c.exec(path, nil, ReadNode)
	if err == nil && nodeJson == "" && !must {
		return nil, nil
	}
	if err != nil {
		log.Debugf("sqlclient create node %s failed: %s", path, err)
		return nil, err
	}
	log.Debugf("sqlClient create OK")
	return []byte(nodeJson), nil
}

func (c *Client) List(path string, must bool) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	
	children, err := c.children(path)
	if err == nil && len(children) == 0 && !must {
		return nil, nil
	}
	if err != nil {
		log.Debugf("mysql list node %s failed: %s", path, err)
		return nil, err
	}

	return children, nil
}

func (c *Client) exec(path string, data []byte, opt string) (string, error) {
	pathList := strings.Split(path[1:], "/")
	pathDeep := len(pathList)
	if  pathDeep < 3 || pathDeep > 4 {
		return "", errors.New("invalid path")
	}

	productName := pathList[1]
	nodeType := "codis3_" + pathList[2]
	sql := ""
	if pathDeep == 3 {
		switch pathList[2] {
		case "topom", "sentinel":
			sql = formatSql(table, productName, nodeType, "", string(data[:]), opt)

		default:
			return "", errors.New("invalid path")
		}
	} else if pathDeep == 4 {
		switch pathList[2] {
		case "topom","sentinel" :
			;

		case "proxy", "group", "slots" :
			sql = formatSql(table, productName, nodeType, pathList[3], string(data[:]), opt)

		default:
			return "", errors.New("invalid path")
		}
	}

	resp := ""
	if opt == ReadNode {
		nodeJson, err := c.querySql(sql)
		if err != nil {
			log.WarnErrorf(err,"exec '%s' failed", sql)
			return "", err
		}
		resp = nodeJson
	} else {
		_, err := c.execSql(sql)
		if err != nil {
			log.WarnErrorf(err,"exec '%s' failed", sql)
			return "", err
		}
	}
	return resp, nil
}

func (c *Client) children(path string) ([]string, error) {
	pathList := strings.Split(path[1:], "/")
	pathDeep := len(pathList)
	if  pathDeep != 3 {
		return nil, errors.New("invalid path")
	}

	productName := pathList[1]
	nodeType := "codis3_" +  pathList[2]
	sql := "select node_name from " + table + " where product_name='" + productName + "' and node_type='" + nodeType + "';"

	rows, err := c.dbmap.Db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []string
	var node = ""
	for rows.Next() {
		err := rows.Scan(&node)
		if err != nil {
			return nil, err
		}
		children = append(children, path + "/" + node)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (c *Client) execSql(sql string) (string, error) {
	_, err := c.dbmap.Db.Exec(sql)
	if err != nil {
		return "", err
	}
	return "", nil
}

func (c *Client) querySql(sql string) (string, error) {
	rows, err := c.dbmap.Db.Query(sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var nodeJson = ""
	for rows.Next() {
		err := rows.Scan(&nodeJson)
		if err != nil {
			return "", err
		}
	}

	err = rows.Err()
	if err != nil {
		return "", err
	}

	return nodeJson, nil
}

/*func formatSqlDeep3(table string, product string, data string, opt string) string {
	sql := ""
	switch opt {
	case ReadNode :
		sql = "select node_json from " + table + " where product_name='" + product + "';"

	case CreateNode, UpdateNode :
		sql = "replace into " + table + " values('" + product + "', '" + data + "');"

	case DeleteNode :
		sql = "delete from " + table + " where product_name='" + product + "';"

	default:
		return ""
	}
	return sql
}*/

func formatSql(table, productName string, nodeType, nodeName string, nodeValue string, opt string) string {
	sql := ""
	switch opt {
	case ReadNode :
		sql = "select node_value from " + table + " where product_name='" + productName + "' and node_type='" + nodeType + "' and node_name='" + nodeName + "';"

	case CreateNode, UpdateNode :
		sql = "insert into " + table + " (product_name, node_type, node_name, node_value) values('" + productName + "', '" + nodeType + "', '" + nodeName + "', '" + nodeValue + "') on duplicate key update product_name='" + productName + "', node_type='" + nodeType + "', node_name='" + nodeName + "', node_value='" + nodeValue + "';"

	case DeleteNode :
		sql = "delete from " + table + " where product_name='" + productName + "' and node_type='" + nodeType + "' and node_name='" + nodeName + "';"

	default:
		return ""
	}
	return sql
}

func (c *Client) WatchInOrder(path string) (<-chan struct{}, []string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, nil, errors.Trace(ErrClosedClient)
	}
	return nil, nil, errors.Trace(ErrNotSupported)
}

func (c *Client) CreateEphemeral(path string, data []byte) (<-chan struct{}, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	return nil, errors.Trace(ErrNotSupported)
}

func (c *Client) CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, "", errors.Trace(ErrClosedClient)
	}
	return nil, "", errors.Trace(ErrNotSupported)
}