// $(document).ready(function () {
// 	console.log(document.cookie);
// 	if(getCookie('user')){
// 		window.location.href = '/main';
// 	}
// });

$(function() {
	'use strict';
  $('.form-control').on('input', function() {
	  var $field = $(this).closest('.form-group');
	  if (this.value) {
	    $field.addClass('field--not-empty');
	  } else {
	    $field.removeClass('field--not-empty');
	  }
	});

});


$('#sb1').click(function (event) {
    console.log('clicked');
	event.preventDefault();
	$(".wrongpass").hide()
	$(".wronguser").hide()
	var username = $('#username').val();
	var password = $('#password-field').val();
	var formData = new FormData();
	formData.append('username', username);
	formData.append('password', password);
	console.log(formData);
	$.ajax({
		type: 'POST',
		url: '/verifylogin',
		data: formData,
		contentType: false,
		processData: false,
		success: function (response) {
			console.log(response);
			if (response.msg == 'success') {
				console.log(response.msg);
				setCookie('user', username, 1)
				console.log(document.cookie);
				window.location.href='/main'
			}else{
				if (response.user == 'right') {
					$('#password-field').addClass('in-error');
					$(".wrongpass").show()
				} else {
					$('#username').addClass('in-error');
					$('#password-field').addClass('in-error')
                    $(".wrongpass").show()
					$(".wronguser").show()
				}
			}
		}
	})

})

function setCookie(name, value, days) {
	var expires = "";
	if (days) {
		var date = new Date();
		date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
		expires = "; expires=" + date.toUTCString();
	} document.cookie = name + "=" + (value || "") + expires + "; path=/";
}
function getCookie(name) {
		var nameEQ = name + "=";
		var ca = document.cookie.split(';');
		for (var i = 0; i < ca.length; i++) {
			var c = ca[i];
			while (c.charAt(0) == ' ') c = c.substring(1, c.length);
			if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
		}
		return null;
	}
function clearAllCookies() {
    var cookies = document.cookie.split(";");
    for (var i = 0; i < cookies.length; i++) {
        var cookie = cookies[i];
        var eqPos = cookie.indexOf("=");
        var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
        document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
    }
}	
