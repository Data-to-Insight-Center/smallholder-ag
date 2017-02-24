//var editor; // use a global for the submit and return data rendering in the contact
var otable;
var dataTab;
var textit_prefix = "./api/zambia";
$(document).ready(function() {
	$.fn.dataTable.ext.errMode = 'none';
	
    editor = new $.fn.dataTable.Editor( {
        table: "#contact",
        fields: [ {
                label: "Contact UUID:",
                name: "uuid",
				type: "readonly"
            }, {
                label: "Contact Name:",
                name: "name",
				type: "readonly"
            }, {
                label: "Phone No:",
                name: "phone",
				type: "readonly"
            }, {
                label: "Country:",
                name: "country",
				type: "readonly"
            }, {
                label: "Province:",
                name: "province",
				fieldInfo: "Ex: Southern"
            }, {
                label: "District:",
                name: "district",
				fieldInfo: "Ex: Choma"
            }, {
                label: "Longitude:",
                name: "longitude",
				fieldInfo: "Ex: 27.8493"
            }, {
                label: "Latitude:",
                name: "latitude",
				fieldInfo: "Ex: 13.1339"
            }, {
                label: "Camp:",
                name: "camp",
				fieldInfo: "Ex: Sedumbwe, Masuka"
            }, {
                label: "UID:",
                name: "uid",
				fieldInfo: "Ex: 25019"
            }, {
                label: "HICPS/COWS:",
                name: "hicps_cows",
				fieldInfo: "Ex: HIPS, COWS"
            }
        ]
    } );
	
	
	$('#contact').on( 'click', 'tbody td:not(:first-child)', function (e) {
        editor.inline( this, {
            buttons: { label: '&gt;', fn: function () { 
			
			var table = $('#contact').DataTable();
			
			$('div .DTE_Form_Buttons').click(function () {			
				
				//var date_enrolled = $("#DTE_Field_date_enrolled").val();		
				var latitude = $("#DTE_Field_latitude").val();
				var province = $("#DTE_Field_province").val();
				var district = $("#DTE_Field_district").val();
				var camp = $("#DTE_Field_camp").val();
				var uid = $("#DTE_Field_uid").val();
				var longitude = $("#DTE_Field_longitude").val();
				var hicps_cows= $("#DTE_Field_hicps_cows").val();
				
				var uuid = $(this).closest('tr').find('td:eq(1)').text();
				
				if(province==undefined){
					province = $(this).closest('tr').find('td:eq(4)').text();
				}if(district==undefined){
					district = $(this).closest('tr').find('td:eq(5)').text();
				}if(longitude==undefined){
					longitude = $(this).closest('tr').find('td:eq(6)').text();
				}if(latitude==undefined){
					latitude = $(this).closest('tr').find('td:eq(7)').text();
				}if(camp==undefined){
					camp = $(this).closest('tr').find('td:eq(8)').text();
				}if(uid==undefined){
					uid = $(this).closest('tr').find('td:eq(9)').text();
				}if(hicps_cows==undefined){
					hicps_cows = $(this).closest('tr').find('td:eq(10)').text();
				}
				
				var dataArray = {};
				dataArray['uuid'] = uuid;
				
				if (latitude != ""){
					dataArray['latitude'] = latitude;
				}if (longitude != ""){
					dataArray['longitude'] = longitude;
				}if (province != ""){
					dataArray['province'] = province;
				}if (district != ""){
					dataArray['district'] = district;
				}if (camp != ""){
					dataArray['camp'] = camp;
				}if (uid != ""){
					dataArray['uid'] = uid;
				}if (hicps_cows != ""){
					dataArray['hicps_cows'] = hicps_cows;
				}
				
				$.ajax({
					type: "POST",
					//in this method insert the data in your database
					url: textit_prefix + "/contacts",
					contentType: "application/json; charset=utf-8",
					data: JSON.stringify(dataArray),
					
					dataType: "json",
					success: AjaxUpdateDataSucceeded,
					error: AjaxUpdateDataFailed
				});
			});
			
			} }
        } );
    } );
 
    $('#contact').DataTable( {
        dom: "Bflrtip",
		lengthMenu: [ 10, 50, 100 ],
        ajax: {
			url: textit_prefix + "/contacts",
            dataSrc : ""
		},
        columns: [
			{
                data: null,
                defaultContent: '',
                className: 'select-checkbox',
                orderable: false
            },
			{ data: "uuid", "sClass":"uuid_hide"},
			{ data: "name" },
			{ data: "phone" },
			{ data: "country", "visible": false },
			{ data: "province" },
			{ data: "district" },
			{ data: "longitude" },
			{ data: "latitude" },
			{ data: "camp" },
			{ data: "uid"},
			{ data: "hicps_cows"}
        ],
		
        select: {
            style:    'os',
            selector: 'td:first-child'
        },
        buttons: [
            { extend: "edit", editor: editor }
        ]
    } );

} );

function AjaxUpdateDataSucceeded(result) {
    if (result != "[]") {
		$('#success').html(""); //Clear error span
    	$('#success').append( "<span class='alert alert-success'><strong>Success! </strong>" + ' ' +" Metadata field successfully updated" + "<br></span>");
		setTimeout(function(){
		   window.location.reload();
		}, 10000);
		
    }
}

function AjaxUpdateDataFailed(result) {
	//alert(result.responseText);
    //alert(result.status + ' ' + result.statusText);
	$('#failure').html(""); //Clear error span
    $('#failure').append("<span class='alert alert-danger'><strong>Failure! </strong>" + ' ' + result.responseText + "<br></span>");
	setTimeout(function(){
		   window.location.reload();
	}, 10000);
}

