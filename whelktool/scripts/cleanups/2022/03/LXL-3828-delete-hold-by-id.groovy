// Remove broken SEK holding. LXL-3828 (Unexpected value of key validation)

selectByIds([
    '2f9w29h701fn2sz4'
]) {
    it.scheduleDelete()
}