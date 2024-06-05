const appendAlert = (message, type,div) => {
    const alertPlaceholder = document.getElementById(div)
    const wrapper = document.createElement('div')
    wrapper.innerHTML = [
        `<div class="alert py-2 alert-${type} alert-dismissible fade show mb-0" role="alert">`,
        `   <div class="alerttext">${message}</div>`,
        '   <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close" style="padding: 1rem 1rem;"></button>',
        '</div>'
    ].join('')
    alertPlaceholder.append(wrapper)
}

