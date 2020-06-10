package com.shanghaiuniversity.ct.web.service;

import com.shanghaiuniversity.ct.web.bean.Calllog;

import java.util.List;

public interface CalllogService {
    List<Calllog> queryMonthDatas(String tel, String calltime);
}
