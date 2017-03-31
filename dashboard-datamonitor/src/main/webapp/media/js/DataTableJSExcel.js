  var tablesToExcel = (function() {
    var uri = 'data:application/vnd.ms-excel;base64,'
    , tmplWorkbookXML = '<?xml version="1.0"?><?mso-application progid="Excel.Sheet"?><Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">'
      + '<DocumentProperties xmlns="urn:schemas-microsoft-com:office:office"><Author>Kunalan Ratharanjan</Author><Created>{created}</Created></DocumentProperties>'
      + '<Styles>'
      + '<Style ss:ID="Currency"><NumberFormat ss:Format="Currency"></NumberFormat></Style>'
      + '<Style ss:ID="Date"><NumberFormat ss:Format="Medium Date"></NumberFormat></Style>'
      + '</Styles>' 
      + '{worksheets}</Workbook>'
    , tmplWorksheetXML = '<Worksheet ss:Name="{nameWS}"><Table>{rows}</Table></Worksheet>'
    , tmplCellXML = '<Cell{attributeStyleID}{attributeFormula}><Data ss:Type="{nameType}">{data}</Data></Cell>'
    , base64 = function(s) { return window.btoa(unescape(encodeURIComponent(s))) }
    , format = function(s, c) { return s.replace(/{(\w+)}/g, function(m, p) { return c[p]; }) }
    return function(tables, wsnames, wbname, appname) {
      var ctx = "";
      var workbookXML = "";
      var worksheetsXML = "";
      var rowsXML = "";
	  
      for (var i = 0; i < tables.length; i++) {
		  
		  rowsXML += '<Row style="color: #ffffff !important;">';
		  for (var h = 0; h < tables[i].columns().data().length; h++) {
            var dataValue1 = null;
			var dataType1 = null;
            var dataStyle1 = null;
          	dataValue1 = (dataValue1)?dataValue1:"<b><i>" + $(tables[i].column(h).header()).html() + "</i></b>";
			var dataFormula1 = null;
            dataFormula1 = (dataFormula1)?dataFormula1:(appname=='Calc' && dataType1=='DateTime')?dataValue1:null;
            ctx = {  attributeStyleID: (dataStyle1=='Currency' || dataStyle1=='Date')?' ss:StyleID="'+dataStyle1+'"':''
                   , nameType: (dataType1=='Number' || dataType1=='DateTime' || dataType1=='Boolean' || dataType1=='Error')?dataType1:'String'
                   , data: (dataFormula1)?'':dataValue1
                   , attributeFormula: (dataFormula1)?' ss:Formula="'+dataFormula1+'"':''
                  };
            rowsXML += format(tmplCellXML, ctx);
		  }
		  rowsXML += '</Row>';
		 // alert($(tables[i]).dataTable().fnGetData().length);
        //if (!tables[i].nodeType) tables[i] = document.getElementById(tables[i]);
        for (var j = 0; j < tables[i].rows().data().length; j++) {
          rowsXML += '<Row>'
          for (var k = 0; k < tables[i].columns().data().length; k++) {
            var dataType = null;
            var dataStyle = null;
            var dataValue = null;
			  
			var header = $(tables[i].column(k).header()).html();
			
            dataValue = (dataValue)?dataValue:tables[i].rows(j).data()[0][header];
            var dataFormula = null;
            dataFormula = (dataFormula)?dataFormula:(appname=='Calc' && dataType=='DateTime')?dataValue:null;
            ctx = {  attributeStyleID: (dataStyle=='Currency' || dataStyle=='Date')?' ss:StyleID="'+dataStyle+'"':''
                   , nameType: (dataType=='Number' || dataType=='DateTime' || dataType=='Boolean' || dataType=='Error')?dataType:'String'
                   , data: (dataFormula)?'':dataValue
                   , attributeFormula: (dataFormula)?' ss:Formula="'+dataFormula+'"':''
                  };
            rowsXML += format(tmplCellXML, ctx);
          }
          rowsXML += '</Row>'
			//var find = ['&','>','<','"'];
			//var replace = ['&amp;','&gt;','&lt;','&quot;'];

			//rowsXML = rowsXML.replace(new RegExp("(" + find.map(function(i){return i.replace(/[.?*+^$[\]\\(){}|-]/g, "\\$&")}).join("|") + ")", "g"), function(s){ return replace[find.indexOf(s)]});
        }
		  
		
        ctx = {rows: rowsXML, nameWS: wsnames[i] || 'Sheet' + i};
        worksheetsXML += format(tmplWorksheetXML, ctx);
        rowsXML = "";
      }

      ctx = {created: (new Date()).getTime(), worksheets: worksheetsXML};
      workbookXML = format(tmplWorkbookXML, ctx);

	  console.log(workbookXML);

      var link = document.createElement("A");
      link.href = uri + base64(workbookXML);
      link.download = wbname || 'Workbook.xls';
      link.target = '_blank';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }
  })();
