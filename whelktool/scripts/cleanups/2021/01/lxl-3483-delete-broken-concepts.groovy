/**
 * Delete a few broken concepts that can't be removed manually.
 *
 * See LXL-3483 for more information.
 */

selectByIds(['zw9dnp7h40687jl', 'xv8cmn6g4zvsx4j', '0xbfpq8j180wzbd', '1zcgqr9k1g72mhn', 'dbqtz11x3xcmqjg']) {
  it.scheduleDelete()
}
