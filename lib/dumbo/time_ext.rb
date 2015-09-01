require 'active_support/core_ext/numeric'

class Time
  def floor(granularity = 1.day)
    Time.at((self.to_f / granularity).floor * granularity)
  end

  def ceil(granularity = 1.day)
    Time.at((self.to_f / granularity).ceil * granularity)
  end
end
