require 'active_support/core_ext/numeric'

class Time
  def floor(granularity = 1.day)
    if granularity == 7.days
      self.at_beginning_of_week
    elsif granularity == 1.month
      self.at_beginning_of_month
    elsif granularity == 1.year
      self.at_beginning_of_year
    else
      Time.at((self.to_f / granularity).floor * granularity)
    end
  end

  def ceil(granularity = 1.day)
    if granularity == 7.days
      self.at_end_of_week.ceil
    elsif granularity == 1.month
      self.at_end_of_month.ceil
    elsif granularity == 1.year
      self.at_end_of_year.ceil
    else
      Time.at((self.to_f / granularity).ceil * granularity)
    end
  end
end
