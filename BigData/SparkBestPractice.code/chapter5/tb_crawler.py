#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urllib, urllib2, base64, json
import random
import logging, datetime, time
import StringIO, gzip

reload(sys)
sys.setdefaultencoding( "utf-8" )

logger = logging.getLogger('debug')
logger.addHandler(logging.StreamHandler(sys.stdout))


# TODO: 根据实际CGI构造链接
# http://s.taobao.com/search?q=%E6%AF%9B%E5%91%A2%E5%A4%96%E5%A5%97+%E9%92%88%E7%BB%87%E8%A1%AB
# next:
#   http://s.taobao.com/search?q=%E6%AF%9B%E5%91%A2%E5%A4%96%E5%A5%97+%E9%92%88%E7%BB%87%E8%A1%AB&bcoffset=1&s=44
# next:
#   http://s.taobao.com/search?q=%E6%AF%9B%E5%91%A2%E5%A4%96%E5%A5%97+%E9%92%88%E7%BB%87%E8%A1%AB&bcoffset=1&s=88



def tb_search(keystr, page=0):

    #_url="http://s.taobao.com/search?q=%C0%E4%B3%D4%CD%C3"
    #print _url
    _url="http://s.taobao.com/search?q=%s" % urllib.quote(keystr)
    if page > 0:
        _url = "%s&bcoffset=1&s=%s" % (_url, page*44)
    print _url

    _headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate, sdch",
        "Accept-Language":"zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4",
        "Cache-Control":"max-age=0",
        "Connection":"keep-alive",
        "Referer":"http://www.taobao.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36"
        }

    req = urllib2.Request(url= _url, headers= _headers)
    f = urllib2.urlopen(req, timeout=10)
    # logger.debug("geturl: %s" % f.geturl())
    # logger.debug("create scanner response: %s info: %s" % (f.getcode(), f.info()))
    if 200 != f.getcode():
        raise Exception("create scanner fail")
    # f=open('/root/book/t.O','r')

    gzip_data=f.read()
    """
    f=open('s.gz','w')
    f.write(gzip_data)
    f.close()
    """
    gzip_stream = StringIO.StringIO(gzip_data)
    f_gzip = gzip.GzipFile(fileobj = gzip_stream)
    # html = f_gzip.read()

    # return

    ks = "    g_page_config = "
    klen = len(ks)
    for l in f_gzip.readlines():
        # print l
        if l[:klen] == ks:
            # print "Found: "
            s=l[klen : len(l)-2]
            # print s[:10]
            # print s[len(s)-10:len(s)]
            return s
            #j=json.loads(s)
            #print j.keys()


def do_key(class_one, class_two, class_three, class_four, page):

    print class_one, class_two, class_three, class_four, page

    q_key="%s %s" % ( class_three, class_four )

    today = datetime.datetime.now().strftime('%Y-%m-%d')

    # q_key="毛呢外套 针织衫"
    # g_page_config=tb_search(q_key)
    g_page_config = tb_search(q_key, page)
    # sys.exit(0)

    ## or read from file
    #f=open('q.1.txt','r')
    #g_page_config=f.read()
    
    
    cfg = json.loads(g_page_config)
    
    # print cfg.keys()
    # print cfg['mods']["itemlist"].keys()
    try:
        for i in cfg['mods']["itemlist"]["data"]["auctions"]:
            # print i["nid"], i["raw_title"] # .decode('unicode')
            # print i.keys()
            #for k in i.keys():
            #    print k, i[k]
            # [u'category', u'raw_title', u'i2iTags', u'user_id', u'title', u'shopcard', u'item_loc', u'reserve_price', u'pid', u'nid', u'nick', u'comment_count', u'view_price', u'view_fee', u'view_sales', u'detail_url', u'shopLink', u'pic_url', u'comment_url', u'icon']
        
            detail_url = "http:" + i["detail_url"]
        
            # goods_base_info
            goods_id = i["nid"]
            goods_name = i["raw_title"]
            store_id = i["user_id"]
            store_name = i['nick']
            # online_time = 
            print "goods_base_info %s|%s|%s|%s|%s|%s|%s" % (goods_id, goods_name, store_id, class_one, class_two, class_three, today)
        
            # store_base_info 
            # store_id, store_name
            is_tmall = 1 if i["shopcard"]["isTmall"] else 0
            location_city = i["item_loc"]
            store_url = i["shopLink"] # 店铺链接
            print "store_base_info %s|%s|%s|%s|%s" % ( store_id, store_name, is_tmall, location_city, today)
        
            # store_credit_info
            credit_as_seller = i["shopcard"]["sellerCredit"]
            score_goods_desc = i["shopcard"]["description"][0]
            score_service_manner = i["shopcard"]["service"][0]
            score_express_speed = i["shopcard"]["delivery"][0]
            # view_price
            print "store_credit_info %s|%s|%s|%s|%s|%s" % (store_id, credit_as_seller, score_goods_desc, score_service_manner, score_express_speed, today)
        
            # goods_sale_info 
            price = i["view_price"]
            express_price = i["view_fee"]
    except:
       pass

    # break;

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print "Usage: %s taobao_class_line_num download_page_num" % sys.argv[0]
        sys.exit(0)

    line_no  = int(sys.argv[1])
    page_num = int(sys.argv[2])
    # input
    """
    class_one = '美食'
    class_two = '中国特色'
    class_three = '西南'
    class_four = '冷吃兔'
    class_one = '女装男装'
    class_two = '女式上装'
    class_three = '毛呢外套'
    class_four = '针织衫'
    """

    f=open('tb_class_2014.utf8.txt', 'r')
    line = 0
    for l in f.readlines():
        line += 1
        if line != line_no:
            continue

        # print l # .decode('utf8')
        arr = l.strip().split("\t")
        # print arr[1], arr[2]

        p=0
        while p < page_num:
            try:
                do_key(arr[1], arr[2], arr[3], arr[4], p)
            except:
                time.sleep(random.uniform(30,60))
            p += 1
            time.sleep(random.uniform(1,3))

        break

    # sys.exit(0)


