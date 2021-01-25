/**
 * Delete broken concepts.
 *
 * //1600-tal
 * hftx5tw13vzq4mb
 *
 * // Ensembler med knäppinstrument
 * dbqtz0hx3dxb2h3
 *
 * //economic conditions
 * jgvxz1t25kg2znj
 *
 * //kao-eng scheme
 * sq47d7fb1fc7sds
 * 1zcgmglk4fmj1h4
 *
 * See LXL-3197 for more information
 */



selectByIds(['hftx5tw13vzq4mb', 'dbqtz0hx3dxb2h3', 'jgvxz1t25kg2znj', 'sq47d7fb1fc7sds', '1zcgmglk4fmj1h4']) {
    it.scheduleDelete()
}